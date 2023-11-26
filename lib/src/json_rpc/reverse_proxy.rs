// Smoldot
// Copyright (C) 2023  Pierre Krieger
// SPDX-License-Identifier: GPL-3.0-or-later WITH Classpath-exception-2.0

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

//!
//! # Usage
//!
//! TODO
//!
//! Call [`ReverseProxy::add_client`] whenever a client connects, and
//! [`ReverseProxy::remove_client`] whenever a client disconnects.
//!
//! # Behavior
//!
//! The behavior is as follows:
//!
//! For the legacy JSON-RPC API:
//!
//! - `system_name` and `system_version` are answered directly by the proxy, and not directed
//! towards any of the servers.
//! - TODO: functions that always error
//! - For all other legacy JSON-RPC API functions: the first time one of these functions is called,
//! it is directed towards a randomly-chosen server. Then, further calls to one of the functions
//! are always redirect to the same server. For example, if the JSON-RPC client calls
//! `chain_subscribeAllHeads` then `chain_getBlock`, these two function calls are guaranteed to be
//! redirected to the same server. If this server is removed, a different server is randomly
//! chosen. Note that there's no mechanism for a server to "steal" clients from another server, as
//! doing so would create inconsistencies from the point of view of the JSON-RPC client. This is
//! considered as a defect in the legacy JSON-RPC API.
//!
//! For the new JSON-RPC API:
//!
//! - `sudo_unstable_version` is answered directly by the proxy, and not directed towards any of
//! the servers.
//! - `sudo_unstable_p2pDiscover` is answered directly by the proxy and is a no-op. TODO: is that correct?
//! - `chainSpec_v1_chainName`, `chainSpec_v1_genesisHash`, and `chainSpec_v1_properties` are
//! redirected towards a randomly-chosen server.
//! - Each call to `chainHead_unstable_follow` is redirected to a randomly-chosen server (possibly
//! multiple different servers for each call). All the other `chainHead` JSON-RPC functions are
//! then redirected to the server corresponding to the provided `followSubscriptionId`. If the
//! server is removed, a `stop` event is generated.
//! - Each call to `transaction_unstable_submitAndWatch` is redirected to a randomly-chosen
//! server. If the server is removed, a `dropped` event is generated.
//!
//! If no server is available, the reverse proxy will delay answering JSON-RPC requests until one
//! server is added (except for the JSON-RPC functions that are answered immediately, as they are
//! always answered immediately).
//!
//! JSON-RPC requests that can't be parsed, use an unknown function, have invalid parameters, etc.
//! are answered immediately by the reverse proxy.
//!
//! If a server misbehaves or returns an internal error, it gets blacklisted and no further
//! requests are sent to it.
//!
//! When a request sent by a client is transferred to a server, the identifier for that request
//! is modified to a randomly-generated identifier. This is necessary because multiple different
//! clients might use the same identifiers for their requests.
//! Similarly, when a server sends a JSON-RPC response containing a subscription ID, the
//! identifier of that subscription is modified to become a randomly-generated value.

// TODO: what about rpc_methods

use alloc::{
    borrow::Cow,
    collections::{btree_map, BTreeMap, BTreeSet, VecDeque},
};
use core::{cmp, mem, ops};
use rand::{seq::IteratorRandom as _, Rng as _};
use rand_chacha::{
    rand_core::{RngCore as _, SeedableRng as _},
    ChaCha20Rng,
};

use crate::json_rpc::methods;

use super::parse;

/// Configuration for a new [`ReverseProxy`].
pub struct Config {
    /// Seed used for randomness. Used to avoid HashDoS attacks and to attribute clients and
    /// requests to servers.
    pub randomness_seed: [u8; 32],
}

/// Configuration for a new [`ReverseProxy`].
pub struct ClientConfig<TClient> {
    /// Maximum number of requests that haven't been answered yet that the client is allowed to
    /// make.
    pub max_unanswered_parallel_requests: usize,

    /// Maximum number of concurrent subscriptions for the legacy JSON-RPC API, except for
    /// `author_submitAndWatchExtrinsic` which is handled by
    /// [`ClientConfig::max_transactions_subscriptions`].
    pub max_legacy_api_subscriptions: usize,

    /// Maximum number of concurrent `chainHead_follow` subscriptions.
    ///
    /// The specification mentions that this value must be at least 2. If the value is inferior to
    /// 2, it is raised.
    pub max_chainhead_follow_subscriptions: usize,

    /// Maximum number of concurrent transactions-related subscriptions.
    pub max_transactions_subscriptions: usize,

    /// Opaque data stored for this client.
    /// Can later be accessed using the `ops::Index` trait implementation of [`ReverseProxy`].
    pub user_data: TClient,
}

/// Reverse proxy state machine. See [..](the module-level documentation).
pub struct ReverseProxy<TClient, TServer> {
    /// List of all clients. Indices serve as [`ClientId`].
    clients: slab::Slab<Client<TClient>>,

    /// Subset of the entries of [`ReverseProxy::clients`] for which
    /// [`Client::server_agnostic_requests_queue`] is non-empty.
    ///
    /// Because the keys are chosen locally, we can use the FNV hasher with no fear of HashDoS
    /// attacks.
    // TODO: call shrink to fit from time to time?
    clients_with_server_agnostic_request_waiting: hashbrown::HashSet<ClientId, fnv::FnvBuildHasher>,

    /// Queues of server-specific requests waiting to be processed.
    /// Indexed by client and by server.
    ///
    /// The queues must never be empty. If a queue is emptied, the item must be removed from
    /// the `BTreeMap` altogether.
    // TODO: call shrink to fit from time to time?
    client_server_specific_requests_queued: BTreeMap<(ClientId, ServerId), VecDeque<QueuedRequest>>,

    /// Same entries as [`ReverseProxy::client_server_specific_requests_queued`], but indexed
    /// differently.
    clients_with_server_specific_request_queued: BTreeSet<(ServerId, ClientId)>,

    /// List of all servers. Indices serve as [`ServerId`].
    servers: slab::Slab<Server<TServer>>,

    /// List of all requests that have been extracted with
    /// [`ReverseProxy::next_proxied_json_rpc_request`] and are being processed by the server.
    ///
    /// Keys are server IDs and the request ID from the point of view of the server. Values are
    /// the client and request from the point of view of the client.
    requests_in_progress: BTreeMap<(ServerId, String), (ClientId, QueuedRequest)>,

    /// List of all subscriptions that are currently active.
    active_subscriptions: BTreeMap<(ServerId, String), (ClientId, String)>,

    /// Same as [`ReverseProxy::active_subscriptions`], by indexed by client.
    active_subscriptions_by_client: BTreeMap<(ClientId, String), (ServerId, String)>,

    /// Source of randomness used for various purposes.
    // TODO: is a crypto secure randomness overkill?
    randomness: ChaCha20Rng,
}

struct Client<TClient> {
    /// Returns the number of requests inserted with
    /// [`ReverseProxy::insert_client_json_rpc_request`] whose response hasn't been pulled with
    /// [`ReverseProxy::next_client_json_rpc_response`] yet.
    num_unanswered_requests: usize,

    /// See [`ClientConfig::max_unanswered_parallel_requests`]
    max_unanswered_parallel_requests: usize,

    /// Number of legacy JSON-RPC API subscriptions that are active, from the moment when the
    /// request to subscribe is inserted in the queue to the moment when the response of the
    /// unsubscription is extracted with [`ReverseProxy::next_client_json_rpc_response`].
    num_legacy_api_subscriptions: usize,

    /// See [`ClientConfig::max_legacy_api_subscriptions`].
    max_legacy_api_subscriptions: usize,

    /// Number of `chainHead_follow` subscriptions that are active, from the moment when the
    /// request to subscribe is inserted in the queue to the moment when the response of the
    /// unsubscription or the `stop` event is extracted with
    /// [`ReverseProxy::next_client_json_rpc_response`].
    num_chainhead_follow_subscriptions: usize,

    /// See [`ClientConfig::max_chainhead_follow_subscriptions`].
    max_chainhead_follow_subscriptions: usize,

    /// Number of `author_submitAndWatchExtrinsic` and `transaction_unstable_submitAndWatch`
    /// subscriptions that are active, from the moment when the request to subscribe is inserted
    /// in the queue to the moment when the response of the unsubscription or the `dropped` event
    /// is extracted with [`ReverseProxy::next_client_json_rpc_response`].
    num_transactions_subscriptions: usize,

    /// See [`ClientConfig::max_transactions_subscriptions`].
    max_transactions_subscriptions: usize,

    /// Queue of JSON-RPC requests inserted with [`ReverseProxy::insert_client_json_rpc_request`]
    /// and that are waiting to be picked up by a server.
    ///
    /// While this queue normally only contains requests that can be sent to any server, it can
    /// contains requests where [`QueuedRequest::is_legacy_api_server_specific`], even when
    /// [`Client::legacy_api_assigned_server`] is `Some`. In that case, when the request is
    /// extracted, it is instead moved to the corresponding server-specific queue.
    // TODO: call shrink to fit from time to time?
    server_agnostic_requests_queue: VecDeque<QueuedRequest>,

    /// Queue of responses waiting to be sent to the client.
    // TODO: call shrink to fit from time to time?
    json_rpc_responses_queue: VecDeque<String>,

    /// Server assigned to this client when it calls legacy JSON-RPC functions. Initially set
    /// to `None`. A server is chosen the first time the client calls a legacy JSON-RPC function.
    legacy_api_assigned_server: Option<ServerId>,

    /// Opaque data chosen by the API user.
    ///
    /// If `None`, then this client is considered non-existent for public-API-related purposes.
    /// This value is set to `None` when the API user wants to remove a client, but that this
    /// client still has active subscriptions that need to be cleaned up.
    user_data: Option<TClient>,
}

struct QueuedRequest {
    id_json: String,

    method: String,

    parameters_json: Option<String>,

    /// `true` if the JSON-RPC function belongs to the category of legacy JSON-RPC functions that
    /// are all redirected to the same server.
    // TODO: consider removing entirely or turning into a proper "type" with an enum
    is_legacy_api_server_specific: bool,
}

struct Server<TServer> {
    /// `true` if the given server has misbehaved and must not process new requests.
    is_blacklisted: bool,

    /// Opaque data chosen by the API user.
    user_data: TServer,
}

/// Identifier of a client within the [`ReverseProxy`].
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientId(usize);

/// Identifier of a server within the [`ReverseProxy`].
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServerId(usize);

impl<TClient, TServer> ReverseProxy<TClient, TServer> {
    /// Initializes a new [`ReverseProxy`].
    pub fn new(config: Config) -> Self {
        ReverseProxy {
            clients: slab::Slab::new(), // TODO: capacity
            clients_iter: 0,
            servers: slab::Slab::new(), // TODO: capacity
            requests_in_progress: BTreeMap::new(),
            randomness: ChaCha20Rng::from_seed(config.randomness_seed),
        }
    }

    pub fn insert_client(&mut self, config: ClientConfig<TClient>) -> ClientId {
        ClientId(self.clients.insert(Client {
            num_unanswered_requests: 0,
            server_agnostic_requests_queue: VecDeque::new(), // TODO: capacity?
            max_unanswered_parallel_requests: config.max_unanswered_parallel_requests,
            num_legacy_api_subscriptions: 0,
            max_legacy_api_subscriptions: config.max_legacy_api_subscriptions,
            num_chainhead_follow_subscriptions: 0,
            max_chainhead_follow_subscriptions: cmp::max(
                config.max_chainhead_follow_subscriptions,
                2,
            ),
            num_transactions_subscriptions: 0,
            max_transactions_subscriptions: config.max_transactions_subscriptions,
            json_rpc_responses_queue: VecDeque::with_capacity(16), // TODO: capacity?
            legacy_api_assigned_server: None,
            user_data: Some(config.user_data),
        }))
    }

    /// Removes a client previously-inserted with [`ReverseProxy::insert_client`].
    ///
    /// Calling this function might generate JSON-RPC requests towards some servers, and
    /// [`ReverseProxy::next_proxied_json_rpc_request`] should be called for every server that
    /// is currently idle.
    pub fn remove_client(&mut self, client_id: ClientId) -> TClient {
        let client = self
            .clients
            .get_mut(client_id.0)
            .unwrap_or_else(|| panic!("client doesn't exist"));
        let user_data = client
            .user_data
            .take()
            .unwrap_or_else(|| panic!("client doesn't exist"));

        // Clear the queue of pending requests of this client. While leaving the queue as-is
        // wouldn't be a problem in terms of logic, we clear it for optimization purposes, in
        // order to avoid processing requests that we know are pointless.
        if !client.server_agnostic_requests_queue.is_empty() {
            let _was_in = self
                .clients_with_server_agnostic_request_waiting
                .remove(&client_id);
            debug_assert!(_was_in);
        } else {
            debug_assert!(!self
                .clients_with_server_agnostic_request_waiting
                .contains(&client_id));
        }
        client.server_agnostic_requests_queue.clear();

        // TODO: remove from client_server_specific_requests_queued as well

        // For each subscription that was active on this client, push a request that unsubscribes.
        for ((_, client_subscription_id), (server_id, server_subscription_id)) in self
            .active_subscriptions_by_client
            .range((client_id, String::new())..(ClientId(client_id.0 + 1), String::new()))
        {
            // TODO:
        }

        // Try to remove the client entirely if possible.
        self.try_remove_client(client_id);

        // Client successfully removed for API-related purposes.
        user_data
    }

    ///
    ///
    /// An error is returned if the JSON-RPC client has queued too many requests that haven't been
    /// answered yet. Try again after a call to [`ReverseProxy::next_client_json_rpc_response`]
    /// has returned `Some`.
    // TODO: proper return value
    pub fn insert_client_json_rpc_request(
        &mut self,
        client_id: ClientId,
        request: &str,
    ) -> Result<(), ()> {
        // Check that the client ID is valid.
        assert!(self.clients[client_id.0].user_data.is_some());

        // Check the limit to the number of unanswered requests.
        if self.clients[client_id.0].num_unanswered_requests
            >= self.clients[client_id.0].max_unanswered_parallel_requests
        {
            return Err(());
        }
        self.clients[client_id.0].num_unanswered_requests += 1;

        // Answer the request directly if possible.
        match methods::parse_jsonrpc_client_to_server(request) {
            Ok((request_id_json, method)) => {
                match method {
                    methods::MethodCall::system_name {} => {
                        self.clients[client_id.0]
                            .json_rpc_responses_queue
                            .push_back(
                                methods::Response::system_name("smoldot-json-rpc-proxy".into())
                                    .to_json_response(request_id_json),
                            );
                        return Ok(());
                    }
                    methods::MethodCall::system_version {} => {
                        self.clients[client_id.0]
                            .json_rpc_responses_queue
                            .push_back(
                            methods::Response::system_version("1.0".into()) // TODO: no
                                .to_json_response(request_id_json),
                        );
                        return Ok(());
                    }
                    methods::MethodCall::sudo_unstable_version {} => {
                        self.clients[client_id.0]
                            .json_rpc_responses_queue
                            .push_back(
                                methods::Response::sudo_unstable_version(
                                    "smoldot-json-rpc-proxy 1.0".into(),
                                ) // TODO: no
                                .to_json_response(request_id_json),
                            );
                        return Ok(());
                    }
                    methods::MethodCall::sudo_unstable_p2pDiscover { .. } => {
                        self.clients[client_id.0]
                            .json_rpc_responses_queue
                            .push_back(
                                methods::Response::sudo_unstable_p2pDiscover(())
                                    .to_json_response(request_id_json),
                            );
                        return Ok(());
                    }
                    methods::MethodCall::state_subscribeRuntimeVersion {}
                    | methods::MethodCall::state_subscribeStorage { .. }
                    | methods::MethodCall::chain_subscribeAllHeads {}
                    | methods::MethodCall::chain_subscribeFinalizedHeads {}
                    | methods::MethodCall::chain_subscribeNewHeads {} => {
                        if self.clients[client_id.0].num_legacy_api_subscriptions
                            >= self.clients[client_id.0].max_legacy_api_subscriptions
                        {
                            // TODO: return error
                            todo!()
                        }

                        self.clients[client_id.0].num_legacy_api_subscriptions += 1;
                    }
                    methods::MethodCall::chainHead_unstable_follow { .. } => {
                        if self.clients[client_id.0].num_chainhead_follow_subscriptions
                            >= self.clients[client_id.0].max_chainhead_follow_subscriptions
                        {
                            // TODO: send back error
                            todo!()
                        }

                        self.clients[client_id.0].num_chainhead_follow_subscriptions += 1;
                    }
                    methods::MethodCall::author_submitAndWatchExtrinsic { .. }
                    | methods::MethodCall::transaction_unstable_submitAndWatch { .. } => {
                        if self.clients[client_id.0].num_transactions_subscriptions
                            >= self.clients[client_id.0].max_transactions_subscriptions
                        {
                            // TODO: send back error
                            todo!()
                        }

                        self.clients[client_id.0].num_transactions_subscriptions += 1;
                    }
                    _ => {}
                }
            }
            Err(methods::ParseClientToServerError::JsonRpcParse(error)) => {
                todo!() // TODO:
            }
            Err(methods::ParseClientToServerError::Method { request_id, error }) => {
                todo!() // TODO:
            }
            Err(methods::ParseClientToServerError::UnknownNotification(function)) => {
                todo!() // TODO:
            }
        };

        Ok(())
    }

    /// Returns the next JSON-RPC response or notification to send to the given client.
    ///
    /// Returns `None` if none is available.
    // TODO: indicate when one might be available
    pub fn next_client_json_rpc_response(&mut self, client_id: ClientId) -> Option<String> {
        assert!(self.clients[client_id.0].user_data.is_some());
        // TODO: decrease num_unanswered_requests if not a notification
        self.clients[client_id.0]
            .json_rpc_responses_queue
            .pop_front()
    }

    /// Adds a new server to the list of servers.
    #[cold]
    pub fn insert_server(&mut self, user_data: TServer) -> ServerId {
        ServerId(self.servers.insert(Server {
            is_blacklisted: false,
            user_data,
        }))
    }

    /// Removes a server from the list of servers.
    ///
    /// All active subscriptions and requests are either stopped or redirected to a different
    /// server.
    #[cold]
    pub fn remove_server(&mut self, server_id: ServerId) -> TServer {
        self.blacklist_server(server_id);
        self.servers.remove(server_id.0).user_data
    }

    #[cold]
    fn blacklist_server(&mut self, server_id: ServerId) {
        // Set `is_blacklisted` to `true`, and return immediately if it was already `true`.
        if mem::replace(&mut self.servers[server_id.0].is_blacklisted, true) {
            return;
        }

        // Extract from `active_subscriptions` the subscriptions that were handled by that server.
        let active_subscriptions = {
            let mut server_and_after = self
                .active_subscriptions
                .split_off(&(server_id, String::new()));
            let mut after = server_and_after.split_off(&(ServerId(server_id.0 + 1), String::new()));
            self.active_subscriptions.append(&mut after);
            server_and_after
        };

        for (_, (client_id, subscription_id)) in active_subscriptions {
            // TODO:
        }

        // TODO: clear server assignments from clients

        // Any active `chainHead_follow` subscription is killed.
        // TODO:

        // Any active `transaction_submitAndWatch` subscription is killed.
        // TODO:

        // Any legacy JSON-RPC API subscription that the server was handling is re-subscribed
        // by adding to the head of the JSON-RPC client requests queue a fake subscription request.
        // TODO:

        // The server-specific requests that were queued for this server and the requests that
        // were already sent to the server are processed the same way, as from the point of view
        // of the JSON-RPC client there's no possible way to differentiate the two.
        let requests_to_cancel = {
            // Extract from `clients_with_server_specific_request_queued` the list of clients with
            // pending requests that can only target that server.
            let clients_with_server_specific_request_queued = {
                let mut server_and_after = self
                    .clients_with_server_specific_request_queued
                    .split_off(&(server_id, ClientId(usize::MIN)));
                let mut after =
                    server_and_after.split_off(&(ServerId(server_id.0 + 1), ClientId(usize::MIN)));
                self.clients_with_server_specific_request_queued
                    .append(&mut after);
                server_and_after
            };

            // Extract from `requests_in_progress` the requests of that server.
            let requests_in_progress = {
                let mut server_and_after = self
                    .requests_in_progress
                    .split_off(&(server_id, String::new()));
                let mut after =
                    server_and_after.split_off(&(ServerId(server_id.0 + 1), String::new()));
                self.requests_in_progress.append(&mut after);
                server_and_after
            };

            let requests_queued = clients_with_server_specific_request_queued
                .into_iter()
                .flat_map(|(_, client_id)| {
                    self.client_server_specific_requests_queued
                        .remove(&(client_id, server_id))
                        .unwrap_or_else(|| unreachable!())
                        .into_iter()
                        .map(|rq| (client_id, rq))
                });
            let requests_dispatched = requests_in_progress
                .into_iter()
                .map(|(_, (client_id, rq))| (client_id, rq));
            requests_dispatched.chain(requests_queued)
        };

        // Note that `requests_to_cancel` is ordered more or less from oldest request to
        // newest request. The exact order is not kept, but given that blacklisting a server is
        // an uncommon operation this is acceptable.
        for (client_id, request_info) in requests_to_cancel {
            // If the client was previously removed by the API user, we simply cancel the request
            // altogether, as if it had never been sent.
            // TODO: what if it's an unsubscribe request
            if self.clients[client_id.0].user_data.is_none() {
                self.clients[client_id.0].num_unanswered_requests -= 1;
                self.try_remove_client(client_id);
                continue;
            }

            // TODO:

            // Any pending request targetting a `chainHead_follow` subscription is answered
            // immediately.
            // TODO:

            // Any pending request targetting a `transaction_submitAndWatch` subscription is answered
            // immediately.
            // TODO:

            // Any other request is added back to the head of the queue of its JSON-RPC client.
            self.clients[client_id.0]
                .server_agnostic_requests_queue
                .push_front(request_info);
        }
    }

    /// Pick a JSON-RPC request waiting to be processed.
    ///
    /// Returns `None` if no JSON-RPC request is waiting to be processed.
    ///
    /// If `None` is returned, you should try calling this function again after
    /// [`ReverseProxy::insert_client_json_rpc_request`] or [`ReverseProxy::remove_client`].
    ///
    /// The JSON-RPC request being returned depends on the server that will process it, as, in
    /// order to preserve the logic of the JSON-RPC API, some requests must be directed towards
    /// the same server that has processed an earlier related request. For example, when a
    /// JSON-RPC client sends a request to unsubscribe, it must be directed to the server that is
    /// handling the subscription, and no other.
    /// For this reason, [`ReverseProxy::next_proxied_json_rpc_request`] should be called for
    /// every (and not just one) idle server after a call to
    /// [`ReverseProxy::insert_client_json_rpc_request`] or [`ReverseProxy::remove_client`].
    // TODO: ^ that's a very shitty requirement, remove
    ///
    /// Note that the [`ReverseProxy`] state machine doesn't enforce any limit to the number of
    /// JSON-RPC requests that a server processes simultaneously. A JSON-RPC server is expected to
    /// back-pressure its socket once it gets too busy, in which case
    /// [`ReverseProxy::next_proxied_json_rpc_request`] should no longer be called until the
    /// server is ready to accept more data.
    /// This ensures that for example a JSON-RPC server that is twice as powerful compared to
    /// another one should get approximately twice the number of requests.
    pub fn next_proxied_json_rpc_request(&mut self, server_id: ServerId) -> Option<String> {
        let server = &mut self.servers[server_id.0];
        if server.is_blacklisted {
            return None;
        }

        // In order to guarantee fairness between the clients, choosing which request to send to
        // the server is done in two steps: first, pick a client that has at least one request
        // waiting to be sent, then pick the first request from that client's queue. This
        // guarantees that a greedy client can't starve the other clients.
        // There are two types of requests: requests that aren't attributed to any server, and
        // requests that are attributed to a specific server. For this reason, the list of clients
        // to pick a request from depends on the server. This complicates fairness.
        // To solve this problem, we join two lists together: the list of clients with at least one
        // server-agnostic request waiting, and the list of clients with at least one
        // server-specific request waiting. The second list is weighted based on the total number
        // of clients and servers. The client to pick a request from is picked randomly from the
        // concatenation of these two lists.

        loop {
            // Choose the client to pick a request from.
            let (client_with_request, pick_from_server_specific) = {
                let mut clients_with_server_specific_request = self
                    .clients_with_server_specific_request_queued
                    .range((server_id, ClientId(usize::MIN))..=(server_id, ClientId(usize::MAX)))
                    .map(|(_, client_id)| *client_id);
                let server_specific_weight: usize =
                    1 + (self.clients.len().saturating_sub(1) / self.servers.len());
                // While we could in theory use `rand::seq::IteratorRandom` with something
                // like `(0..server_specific_weight).flat_map(...)`, it's hard to guarantee
                // that doing so would be `O(1)`. Since we want this guarantee, we do it manually.
                let total_weight = self
                    .clients_with_server_agnostic_request_waiting
                    .len()
                    .saturating_add(
                        server_specific_weight
                            .saturating_mul(clients_with_server_specific_request.clone().count()),
                    );
                let index = self.randomness.gen_range(0..total_weight);
                if index < self.clients_with_server_agnostic_request_waiting.len() {
                    let client = *self
                        .clients_with_server_agnostic_request_waiting
                        .iter()
                        .nth(index)
                        .unwrap();
                    (client, false)
                } else {
                    let client = clients_with_server_specific_request
                        .nth(
                            (index - self.clients_with_server_agnostic_request_waiting.len())
                                / server_specific_weight,
                        )
                        .unwrap();
                    (client, true)
                }
            };

            // Extract a request from that client.
            let queued_request = if pick_from_server_specific {
                let Some(requests_queue) = self
                    .client_server_specific_requests_queued
                    .get_mut(&(client_with_request, server_id))
                else {
                    // A panic here indicates a bug in the code.
                    unreachable!()
                };

                let Some(request) = requests_queue.pop_front() else {
                    // A panic here indicates a bug in the code.
                    unreachable!()
                };

                // We need to update caches if this was the last request in queue.
                if requests_queue.is_empty() {
                    self.client_server_specific_requests_queued
                        .remove(&(client_with_request, server_id));
                    self.clients_with_server_specific_request_queued
                        .remove(&(server_id, client_with_request));
                }

                request
            } else {
                let Some(request) = self.clients[client_with_request.0]
                    .server_agnostic_requests_queue
                    .pop_front()
                else {
                    // A panic here indicates a bug in the code.
                    unreachable!()
                };

                // We need to update caches if this was the last request in queue.
                if self.clients[client_with_request.0]
                    .server_agnostic_requests_queue
                    .is_empty()
                {
                    self.clients_with_server_agnostic_request_waiting
                        .remove(&client_with_request);
                }

                // In case where the request is a legacy JSON-RPC API function that gets
                // redirected to a specific server, there are three possibilities:
                //
                // - The client is already attributed to the server. All good, nothing more to do.
                // - The client is not attributed to any server yet. Attribute it to the current
                //   server.
                // - The client is attributed to a different server. This can happen if there is
                //   more than one legacy JSON-RPC API function in queue at the time when the
                //   client was attributed. In that situation, queue back the request that was
                //   just pulled but to the proper server this time.
                //
                if request.is_legacy_api_server_specific {
                    if let Some(other_server) =
                        self.clients[client_with_request.0].legacy_api_assigned_server
                    {
                        if other_server != server_id {
                            // The client is attributed to a different server. Queue back
                            // the request.
                            //
                            // Note that the API user must later call
                            // `next_proxied_json_rpc_request` again with that other server in
                            // order for the request to be picked up. As documented, the API user
                            // is supposed to wake up every idle server when a client inserts a
                            // request. This means that `next_proxied_json_rpc_request` is
                            // necessarily called with that other server, and, because the request
                            // was previously in the server-agnostic queue, that other call could
                            // have picked up the request as well. The fact that it didn't means
                            // either that the call hasn't happened yet, or that the call happened
                            // but the server picked up a different request and was too busy to
                            // pick up the request that interests us. In both cases the call will
                            // happen (again or for the first time) later and everything will work
                            // as intended, except for the small fact that fairness isn't
                            // properly enforced in that niche situation.
                            match self
                                .client_server_specific_requests_queued
                                .entry((client_with_request, other_server))
                            {
                                btree_map::Entry::Occupied(entry) => entry.into_mut(),
                                btree_map::Entry::Vacant(entry) => {
                                    let _was_inserted = self
                                        .clients_with_server_specific_request_queued
                                        .insert((other_server, client_with_request));
                                    debug_assert!(_was_inserted);
                                    entry.insert(VecDeque::new()) // TODO: capacity?
                                }
                            }
                            .push_front(request);
                            continue;
                        }
                    }

                    if self.clients[client_with_request.0]
                        .legacy_api_assigned_server
                        .is_none()
                    {
                        // The client is not attributed to any server yet. Attribute it to the
                        // current server.
                        self.clients[client_with_request.0].legacy_api_assigned_server =
                            Some(server_id);
                    }
                }

                request
            };

            // At this point, we have extracted a request from the queue.

            // The next step is to generate a new request ID and rewrite the request in order to
            // change the request ID.
            let new_request_id = hex::encode({
                let mut bytes = [0; 48];
                self.randomness.fill_bytes(&mut bytes);
                bytes
            });
            let request_with_adjusted_id = parse::build_request(&parse::Request {
                id_json: Some(&new_request_id),
                method: &queued_request.method,
                params_json: queued_request.parameters_json.as_deref(),
            });

            // Update `self` to track that the server is processing this request.
            let _previous_value = self.requests_in_progress.insert(
                (server_id, new_request_id),
                (client_with_request, queued_request),
            );
            debug_assert!(_previous_value.is_none());

            // Success.
            break Some(request_with_adjusted_id);
        }
    }

    /// Inserts a response or notification sent by a server.
    ///
    /// Note that there exists no back-pressure system here. Responses and notifications sent by
    /// servers are always accepted and buffered in order to be picked up later
    /// by [`ReverseProxy::next_client_json_rpc_response`].
    ///
    /// This cannot lead to an excessive memory usage, because the number of responses is bounded
    /// by the maximum number of in-flight JSON-RPC requests enforced on clients, and the
    /// number of notifications is bounded by the maximum number of active subscriptions enforced
    /// on clients. The state machine might merge multiple notifications from the same
    /// subscription into one in order to enforce this bound.
    pub fn insert_proxied_json_rpc_response(
        &mut self,
        server_id: ServerId,
        response: &str,
    ) -> InsertProxiedJsonRpcResponseOutcome {
        match parse::parse_response(response) {
            Ok(parse::Response::ParseError { .. })
            | Ok(parse::Response::Error {
                error_code: -32603, // Internal server error.
                ..
            }) => {
                // JSON-RPC server has returned an "internal server error" or indicates that it
                // has failed to parse our JSON-RPC request as a valid request. This is never
                // supposed to happen and indicates that something is very wrong with the server.

                // The server is blacklisted. While the response might correspond to a request,
                // we do the blacklisting without removing that request from the state, as the
                // blacklisting will automatically remove all requests.
                self.blacklist_server(server_id);
                InsertProxiedJsonRpcResponseOutcome::Blacklisted("") // TODO:
            }
            Ok(parse::Response::Success {
                id_json,
                result_json,
            }) => {
                // TODO: to_owned overhead
                let Some((client_id, request_info)) = self
                    .requests_in_progress
                    .remove(&(server_id, id_json.to_owned()))
                else {
                    // Server has answered a non-existing request. Blacklist it.
                    self.blacklist_server(server_id);
                    return InsertProxiedJsonRpcResponseOutcome::Blacklisted("");
                    // TODO: ^
                };

                // TODO: add subscription if this was a subscribe request
                // TODO: remove subscription if this was an unsubscribe request

                // It is possible that this notification concerns a client that has been
                // destroyed by the API user, in which case we simply discard the
                // notification.
                if self.clients[client_id.0].user_data.is_none() {
                    // Remove the client for real if it was previously removed by the API user and
                    // that this was its last request.
                    self.try_remove_client(client_id);
                    return InsertProxiedJsonRpcResponseOutcome::Discarded;
                }

                // Rewrite the request ID found in the response in order to match what the
                // client expects.
                let response_with_translated_id =
                    parse::build_success_response(&request_info.id_json, result_json);
                self.clients[client_id.0]
                    .json_rpc_responses_queue
                    .push_back(response_with_translated_id);

                // Success.
                InsertProxiedJsonRpcResponseOutcome::Ok(client_id)
            }
            Ok(parse::Response::Error {
                id_json,
                error_code,
                error_message,
                error_data_json,
            }) => {
                // JSON-RPC server has returned an error for this JSON-RPC call.
                // TODO: translate ID
                parse::build_error_response(
                    id_json,
                    parse::ErrorResponse::ApplicationDefined(error_code, error_message),
                    error_data_json,
                );
                // TODO: ?!
            }
            Err(response_parse_error) => {
                match methods::parse_notification(response) {
                    Ok(mut notification) => {
                        // This is a subscription notification.
                        // Because clients are removed only after they have finished
                        // unsubscribing, it is guaranteed that the client is still in the
                        // list.
                        // TODO: overhead of into_owned
                        let Some((client_id, client_notification_id)) = self
                            .active_subscriptions
                            .get(&(server_id, notification.subscription().clone().into_owned()))
                        else {
                            // The subscription ID isn't recognized. This indicates something very
                            // wrong with the server. We handle this by blacklisting the server.
                            self.blacklist_server(server_id);
                            return InsertProxiedJsonRpcResponseOutcome::Blacklisted("");
                            // TODO: ^
                        };

                        // It is possible that this notification concerns a client that has been
                        // destroyed by the API user, in which case we simply discard the
                        // notification.
                        if self.clients[client_id.0].user_data.is_none() {
                            return InsertProxiedJsonRpcResponseOutcome::Discarded;
                        }

                        // Rewrite the subscription ID in the notification in order to match what
                        // the client expects.
                        notification.set_subscription(Cow::Borrowed(&client_notification_id));
                        let rewritten_notification =
                            notification.to_json_request_object_parameters(None);
                        // TODO: must handle situation where client doesn't pull its data
                        self.clients[client_id.0]
                            .json_rpc_responses_queue
                            .push_back(rewritten_notification);

                        // Success
                        InsertProxiedJsonRpcResponseOutcome::Ok(*client_id)
                    }
                    Err(notification_parse_error) => {
                        // Failed to parse the message from the JSON-RPC server.
                        self.blacklist_server(server_id);
                        InsertProxiedJsonRpcResponseOutcome::Blacklisted("")
                        // TODO: ^
                    }
                }
            }
        }
    }

    /// Checks if the given client has been removed using [`ReverseProxy::remove_client`] and that
    /// it has no request in progress and no active subscription, and if so removes it entirely
    /// from the state of `self`.
    fn try_remove_client(&mut self, client_id: ClientId) {
        if self.clients[client_id.0].user_data.is_some() {
            return;
        }

        if self.clients[client_id.0].num_unanswered_requests != 0 {
            return;
        }

        debug_assert!(!self
            .clients_with_server_agnostic_request_waiting
            .contains(&client_id));
        debug_assert!(self
            .client_server_specific_requests_queued
            .range(
                (client_id, ServerId(usize::MIN))
                    ..(ClientId(client_id.0 + 1), ServerId(usize::MIN))
            )
            .next()
            .is_none());

        if self
            .active_subscriptions_by_client
            .range((client_id, String::new())..(ClientId(client_id.0 + 1), String::new()))
            .next()
            .is_some()
        {
            return;
        }

        self.clients.remove(client_id.0);
    }
}

impl<TClient, TServer> ops::Index<ClientId> for ReverseProxy<TClient, TServer> {
    type Output = TClient;

    fn index(&self, id: ClientId) -> &TClient {
        self.clients[id.0].user_data.as_ref().unwrap()
    }
}

impl<TClient, TServer> ops::IndexMut<ClientId> for ReverseProxy<TClient, TServer> {
    fn index_mut(&mut self, id: ClientId) -> &mut TClient {
        self.clients[id.0].user_data.as_mut().unwrap()
    }
}

impl<TClient, TServer> ops::Index<ServerId> for ReverseProxy<TClient, TServer> {
    type Output = TServer;

    fn index(&self, id: ServerId) -> &TServer {
        &self.servers[id.0].user_data
    }
}

impl<TClient, TServer> ops::IndexMut<ServerId> for ReverseProxy<TClient, TServer> {
    fn index_mut(&mut self, id: ServerId) -> &mut TServer {
        &mut self.servers[id.0].user_data
    }
}

pub enum InsertProxiedJsonRpcResponseOutcome {
    Ok(ClientId),
    Discarded,
    Blacklisted(&'static str),
}

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

use std::{
    ffi::{CStr, CString},
    num::NonZeroU32,
    ptr,
    sync::{Arc, Mutex},
};

#[no_mangle]
pub unsafe extern "C" fn smoldot_add_chain(chain_spec: *const libc::c_char) -> libc::size_t {
    let specification = CStr::from_ptr(chain_spec)
        .to_str()
        .unwrap_or_else(|_| panic!("non-utf8 chain specification"));

    let mut global_state = global_state().lock().unwrap();
    let global_state = &mut *global_state; // Avoids borrowing errors.

    let smoldot_light::AddChainSuccess {
        chain_id,
        json_rpc_responses,
    } = global_state
        .client
        .add_chain(smoldot_light::AddChainConfig {
            user_data: (),
            specification,
            database_content: "", // TODO:
            potential_relay_chains: global_state.chain_json_rpc.keys().copied(),
            json_rpc: smoldot_light::AddChainConfigJsonRpc::Enabled {
                max_pending_requests: NonZeroU32::new(u32::max_value()).unwrap(),
                max_subscriptions: u32::max_value(),
            },
        })
        .unwrap();

    global_state
        .chain_json_rpc
        .insert(chain_id, Arc::new(Mutex::new(json_rpc_responses.unwrap())));

    usize::from(chain_id)
}

#[no_mangle]
pub unsafe extern "C" fn smoldot_remove_chain(chain_id: libc::size_t) {
    let chain_id = smoldot_light::ChainId::from(chain_id);

    let mut global_state = global_state().lock().unwrap();

    global_state
        .chain_json_rpc
        .remove(&chain_id)
        .unwrap_or_else(|| panic!("unknown chain"));
    let () = global_state.client.remove_chain(chain_id);
}

#[no_mangle]
pub unsafe extern "C" fn smoldot_json_rpc_request(
    chain_id: libc::size_t,
    json_rpc_request: *const libc::c_char,
) {
    let json_rpc_request = CStr::from_ptr(json_rpc_request)
        .to_str()
        .unwrap_or_else(|_| panic!("non-utf8 json-rpc request"));
    let chain_id = smoldot_light::ChainId::from(chain_id);

    let mut global_state = global_state().lock().unwrap();
    let global_state = &mut *global_state; // Avoids borrowing errors.

    global_state
        .client
        .json_rpc_request(json_rpc_request, chain_id)
        .unwrap();
}

#[no_mangle]
pub unsafe extern "C" fn smoldot_wait_next_json_rpc_response(chain_id: libc::size_t) -> *const libc::c_char {
    let chain_id = smoldot_light::ChainId::from(chain_id);

    let json_rpc_responses = {
        let global_state = global_state().lock().unwrap();
        global_state
            .chain_json_rpc
            .get(&chain_id)
            .unwrap_or_else(|| panic!("unknown chain"))
            .clone()
    };

    let mut json_rpc_responses = json_rpc_responses.lock().unwrap();

    let Some(json_rpc_response) = futures_lite::future::block_on(json_rpc_responses.next()) else {
        return ptr::null_mut();
    };

    let cstring = CString::new(json_rpc_response)
        .unwrap_or_else(|_| panic!("nul character in json-rpc response"));
    cstring.into_raw()
}

#[no_mangle]
pub unsafe extern "C" fn smoldot_next_json_rpc_response_free(s: *const libc::c_char) {
    if s.is_null() {
        panic!("null pointer passed to smoldot_next_json_rpc_response_free")
    }

    let _ = CString::from_raw(s as *mut _);
}

fn global_state() -> &'static Mutex<GlobalState> {
    static CLIENT: async_lock::OnceCell<Mutex<GlobalState>> = async_lock::OnceCell::new();

    CLIENT.get_or_init_blocking(|| {
        // TODO: remove this: https://github.com/smol-dot/smoldot/issues/756
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

        Mutex::new(GlobalState {
            client: smoldot_light::Client::new(
                smoldot_light::platform::default::DefaultPlatform::new(
                    env!("CARGO_PKG_NAME").into(),
                    env!("CARGO_PKG_VERSION").into(),
                ),
            ),
            chain_json_rpc: hashbrown::HashMap::with_hasher(Default::default()),
        })
    })
}

struct GlobalState {
    client: smoldot_light::Client<Arc<smoldot_light::platform::DefaultPlatform>>,
    // TODO: remove this field after the `JsonRpcResponses` type has disappeared: https://github.com/smol-dot/smoldot/issues/735
    chain_json_rpc: hashbrown::HashMap<
        smoldot_light::ChainId,
        Arc<Mutex<smoldot_light::JsonRpcResponses>>,
        fnv::FnvBuildHasher,
    >,
}

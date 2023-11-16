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

#include <stddef.h>

// TODO: needs documentation

size_t smoldot_add_chain(const char* chain_spec);
void smoldot_remove_chain(size_t chain_id);
void smoldot_json_rpc_request(size_t chain_id, const char* json_rpc_request);
const char* smoldot_wait_next_json_rpc_response(size_t chain_id);
void smoldot_next_json_rpc_response_free(const char* response);

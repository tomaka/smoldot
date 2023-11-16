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

#include <stdio.h>
#include <stdlib.h>
#include "smoldot.h"

int main()
{
    // Read the chain specification into `buffer`.
    char *buffer = 0;
    long length;
    FILE *f = fopen("../../demo-chain-specs/polkadot.json", "rb");
    if (!f)
    {
        printf("couldn't open chain spec file");
        return -1;
    }
    fseek(f, 0, SEEK_END);
    length = ftell(f);
    fseek(f, 0, SEEK_SET);
    buffer = malloc(length + 1);
    fread(buffer, 1, length, f);
    fclose(f);
    buffer[length] = '\0';

    // Now actually start using smoldot.

    size_t chain_id = smoldot_add_chain(buffer);
    smoldot_json_rpc_request(chain_id, "{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"chain_subscribeNewHeads\",\"params\":[]}");

    while (1)
    {
        const char *response = smoldot_wait_next_json_rpc_response(chain_id);
        printf("JSON-RPC response: %s\n", response);
        smoldot_next_json_rpc_response_free(response);
    }

    smoldot_remove_chain(chain_id);
    return 0;
}

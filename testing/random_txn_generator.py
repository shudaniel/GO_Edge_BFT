import random
import argparse
import json

random.seed(10)


def generate_txns(num_t, num_c, percent, num_zones, do_baseline):

    # If baseline is set to true, then everyone is part of zone 0

    txndata_for_server = []
    txndata_for_client = {}
    client_throughput = {}
    for i in range(num_zones):
        
        zone = str(i)

        txndata_for_client[zone] = {}
        for j in range(num_c):
            client_id = str((i * num_c) + j)

            client_throughput[client_id] = { "received_txns": 0, "total_latency": 0 }
            new_txn_list = ""
            if i == 0 or do_baseline:
                # NOTE: zone 0 will only get local transactions
                client_local = { "zone": "0", "clientid": client_id, "numtxn": num_t }
                txndata_for_server.append(client_local)

                if num_t >= 1:
                    new_txn_list = "l" + (",l" * (num_t - 1))
            else:

                client_local = { "zone": zone, "clientid": client_id, "numtxn": 0 }
                client_global = { "zone": "0", "clientid": client_id, "numtxn": 0 }

                for k in range(num_t):
                    
                    rand = random.random()
                    txn_type = ""
                    if rand < percent:

                        txn_type = "g"
                        client_global["numtxn"] += 1
                    else:
                        txn_type = "l"
                        client_local["numtxn"] += 1
                    
                    if k == 0:
                        new_txn_list  = txn_type
                    else:
                        new_txn_list += "," + txn_type


                txndata_for_server.append(client_local)
                txndata_for_server.append(client_global)
            txndata_for_client[zone][client_id] = new_txn_list
    

    return json.dumps(txndata_for_server), txndata_for_client, client_throughput
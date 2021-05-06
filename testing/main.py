# This file signals all the client threads in the different zones to start and also to collect all the total times
import socket
import time
import os
import json
import argparse
import threading
import re
from random_txn_generator import generate_txns

lock = threading.Lock()

def connect_to_primary(addr, port, primary_info, txns_list_for_server, client_throughput, do_baseline, stats):
    global lock

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # print("Listening on", addr, port)
    # sock.bind((addr, port))
  
    # Connect with the primaries via tcp
    if not do_baseline or primary_info["zone"] == "0":
        target_addr = (primary_info["ip"], int(primary_info["port"]))
        sock.connect(target_addr)
        sock.recv(1024) # Ignore the JOIN message
        msg = "*TXN_DATA|" + txns_list_for_server + "|~*"
        encoded_msg = msg.encode('utf-8')
        print("sending TXN_DATA to", target_addr, "of len", len(encoded_msg))

        sock.send(encoded_msg)
        # Send in 1024 byte chunks
        # start = 0
        # while start + 1024 <= len(encoded_msg):
        #     sock.sendto( encoded_msg[start:(start + 1024)] , target_addr)
        #     start += 1024
        #     time.sleep(0.1)
        
        # # One more fragment to send
        # if start < len(encoded_msg):
        #     sock.sendto( encoded_msg[start:] , target_addr)
    
        print("Done")
        message = ""

        regex_match = r'^[a-zA-Z0-9_:!|.;,~/{}"\[\] ]*$'
        while True:
            data = sock.recv(8196)
            msg = data.decode()
            msg_split = msg.split("*")
            for msg_component in msg_split:
                if len(msg_component) == 0 or not re.match(regex_match, msg_component):
                    continue
                message = message + msg_component
                if msg_component[-1] != "~":
                    continue
                else:
                    message = message[:-1]
                    lock.acquire()
                    json_data = json.loads(message)
                    for clientid in json_data:
                        stats["total_latency"] += json_data[clientid]["totallatency"]
                        stats["total_txn"] += json_data[clientid]["numtxn"]
                        client_throughput[clientid]["total_latency"] += json_data[clientid]["totallatency"]
                        client_throughput[clientid]["received_txns"] += json_data[clientid]["numtxn"]
                    print("Total Latency:", stats["total_latency"], "|Total txn:", stats["total_txn"])
                    total_throughput = 0
                    for clientid in client_throughput:
                        if client_throughput[clientid]["received_txns"] > 0:
                            total_throughput += client_throughput[clientid]["received_txns"] / client_throughput[clientid]["total_latency"]
                    print("Total Throughput:", total_throughput)

                    lock.release()
                    message = ""
                

    

parser = argparse.ArgumentParser()
parser.add_argument("--address", "-a", default="127.0.0.1")
parser.add_argument("--port", "-p",  type=int, default=8000)
parser.add_argument("--numclients", "-c", type=int, default=1)
parser.add_argument("--numtransactions", "-t", type=int, default=10)
parser.add_argument("--percent", "-r", type=float, default=0.1)
parser.add_argument("--baseline", "-b", type=int, default=0)
args = parser.parse_args()


# clients = {
#     "0": ("127.0.0.1", 7000),  
#     "1": ("127.0.0.1", 7100), 
#     "2": ("127.0.0.1", 7200), 
# }

stats = {
    "total_latency": 0.0,
    "total_txn": 0
}

clients = {
    "0": ("54.241.99.239", 8000),  
    "1": ("3.138.116.243", 8000), 
    "2": ("35.182.100.47", 8000), 
}


sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((args.address, args.port))

do_baseline = args.baseline == 1
txns_list_for_server, txns_dict_for_client, client_throughput = generate_txns(num_t = args.numtransactions, num_c = args.numclients, percent = args.percent, num_zones = len(clients.keys()), do_baseline = do_baseline)
with open("primaries.json", "r") as readfile:
    primary_info = json.loads(readfile.read())
    for i in range(len(primary_info)):
        threading.Thread(target=connect_to_primary, args=(args.address, args.port + i + 1, primary_info[i], txns_list_for_server, client_throughput, do_baseline, stats,)).start()
#         # Connect with the primaries via tcp
#         if not do_baseline or primary_info[i]["zone"] == "0":
#             target_addr = (primary_info[i]["ip"], int(primary_info[i]["port"]))
#             msg = "*TXN_DATA|" + txns_list_for_server + "|~*"
#             encoded_msg = msg.encode('utf-8')
#             print("sending TXN_DATA to", target_addr, "of len", len(encoded_msg))
#             # Send in 1024 byte chunks
#             start = 0
#             while start + 1024 <= len(encoded_msg):
#                 sock.sendto( encoded_msg[start:(start + 1024)] , target_addr)
#                 start += 1024
#                 time.sleep(0.1)
            
#             # One more fragment to send
#             if start < len(encoded_msg):
#                 sock.sendto( encoded_msg[start:] , target_addr)




start = input("Push any key to start\n")
# First, send a reset signal to everyone
# reset_msg = "RESET".encode('utf-8')
# if os.path.exists("sample.json"):
#     with open("sample.json", "r") as readfile:
#         addresses = json.loads(readfile.read())
#         for addr in addresses:
#             sock.sendto(reset_msg, (addr[0], addr[1]))



for zone in clients:
    # Signal all the client_masters to start
    msg = zone + "|" + str(args.numclients) + "|" + str(args.numtransactions) + "|" + str(args.percent) + "|" + json.dumps(txns_dict_for_client[zone]) + "|" + str(args.baseline) + "|~"
    
    index = 0

    while index + 2046 <= len(msg): 

        fragment = "*" + msg[index:(index + 2046)] + "*"
        sock.sendto( fragment.encode('utf-8'), clients[zone])
        index += 2046
        time.sleep(0.1)
    if index < len(msg):
        fragment = "*" + msg[index:] + "*"
        sock.sendto(fragment.encode('utf-8'), clients[zone])

start = input("Push any key to start again")
start_time = time.time()
for zone in clients:
    startmsg = "start".encode('utf-8')
    sock.sendto(startmsg, clients[zone])

earliest = 0
latest = 0

old_latency_total = 0

while True:
    data, addr = sock.recvfrom(8196)
    # endtime = time.time()
    msg = data.decode()[:-1]
    # print("Received times:", msg)
    msg_split = msg.split("|")

    # if len(msg_split) == 1:
    #     json_data = json.loads(msg)
    #     for clientid in json_data:
    #         total_latency += json_data[clientid]["totallatency"]
    #         total_txn += json_data[clientid]["numtxn"]
    #         client_throughput[clientid]["total_latency"] += json_data[clientid]["totallatency"]
    #         client_throughput[clientid]["received_txns"] += json_data[clientid]["numtxn"]
        
    #     print("Total Latency:", total_latency, "|Total txn:", total_txn)
    #     total_throughput = 0
    #     for clientid in client_throughput:
    #         if client_throughput[clientid]["received_txns"] > 0:
    #             total_throughput += client_throughput[clientid]["received_txns"] / client_throughput[clientid]["total_latency"]
    #     print("Total Throughput:", total_throughput)

    # else:

    old_latency_total += float(msg_split[0])
    print("Total Latency (old):", old_latency_total)
        # # start_time = float(msg_split[1])
        # # print("Value from before:", endtime - start_time)
        # end = int(msg_split[2])/1000000000
        # start = int(msg_split[1])/1000000000
        # if end > latest:
        #     latest = end
        # if earliest == 0 or start < earliest:
        #     earliest = start
        # print("Total time:", latest - earliest)

# This file signals all the client threads in the different zones to start and also to collect all the total times
import socket
import time
import os
import json
import argparse
from random_txn_generator import generate_txns

parser = argparse.ArgumentParser()
parser.add_argument("--address", "-a", default="127.0.0.1")
parser.add_argument("--port", "-p",  type=int, default=8000)
parser.add_argument("--numclients", "-c", type=int, default=1)
parser.add_argument("--numtransactions", "-t", type=int, default=10)
parser.add_argument("--percent", "-r", type=float, default=0.1)
args = parser.parse_args()


clients = {
    "0": ("127.0.0.1", 7000),  
    "1": ("127.0.0.1", 7100), 
    "2": ("127.0.0.1", 7200), 
}


# clients = {
#     "0": ("", 8000),  
#     "1": ("", 8000), 
#     "2": ("", 8000), 
# }


sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((args.address, args.port))


txns_list_for_server, txns_dict_for_client = generate_txns(num_t = args.numtransactions, num_c = args.numclients, percent = args.percent, num_zones = len(clients.keys()))
with open("primaries.json", "r") as readfile:
    primary_info = json.loads(readfile.read())
    for i in range(len(primary_info)):
        # Connect with the primaries via tcp
        target_addr = (primary_info[i]["ip"], int(primary_info[i]["port"]))
        msg = "*TXN_DATA|" + txns_list_for_server + "|~*"
        print("sending", msg, "to", target_addr)
        sock.sendto( msg.encode('utf-8')  , target_addr)


start = input("Push any key to start")
# First, send a reset signal to everyone
# reset_msg = "RESET".encode('utf-8')
# if os.path.exists("sample.json"):
#     with open("sample.json", "r") as readfile:
#         addresses = json.loads(readfile.read())
#         for addr in addresses:
#             sock.sendto(reset_msg, (addr[0], addr[1]))



for zone in clients:
    # Signal all the client_masters to start
    msg = zone + "|" + str(args.numclients) + "|" + str(args.numtransactions) + "|" + str(args.percent) + "|" + json.dumps(txns_dict_for_client[zone])
    startmsg = msg.encode('utf-8')
    sock.sendto(startmsg, clients[zone])

start = input("Push any key to start again")
start_time = time.time()
for zone in clients:
    startmsg = "start".encode('utf-8')
    sock.sendto(startmsg, clients[zone])

earliest = 0
latest = 0
while True:
    data, addr = sock.recvfrom(1024)
    # endtime = time.time()
    msg = data.decode()
    print("Received times:", msg)
    # msg_split = msg.split("|")

    # # start_time = float(msg_split[1])
    # # print("Value from before:", endtime - start_time)
    # end = int(msg_split[2])/1000000000
    # start = int(msg_split[1])/1000000000
    # if end > latest:
    #     latest = end
    # if earliest == 0 or start < earliest:
    #     earliest = start
    # print("Total time:", latest - earliest)

# This file signals all the client threads in the different zones to start and also to collect all the total times
import socket
import time
import os
import json
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--address", "-a", default="127.0.0.1")
parser.add_argument("--port", "-p",  type=int, default=8000)
args = parser.parse_args()


clients = [
    # ("127.0.0.1", 7000),
    # ("127.0.0.1", 7100),
    # ("127.0.0.1", 7200),
    ("54.183.5.97	", 8000),
    ("18.191.108.212", 8000),
    ("99.79.42.201", 8000)
]

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((args.address, args.port))


start = input("Push any key to start")
# First, send a reset signal to everyone
# reset_msg = "RESET".encode('utf-8')
# if os.path.exists("sample.json"):
#     with open("sample.json", "r") as readfile:
#         addresses = json.loads(readfile.read())
#         for addr in addresses:
#             sock.sendto(reset_msg, (addr[0], addr[1]))



for i in range(len(clients)):
    startmsg = "start".encode('utf-8')
    print(clients[i])
    sock.sendto(startmsg, clients[i])

start = input("Push any key to start again")
start_time = time.time()
for i in range(len(clients)):
    startmsg = "start".encode('utf-8')
    print(clients[i])
    sock.sendto(startmsg, clients[i])

earliest_starttime = 0
latest_endtime = 0
while True:
    data, addr = sock.recvfrom(1024)
    endtime = time.time()
    msg = data.decode()
    print("Received times:", msg)
    msg_split = msg.split("|")
    start = int(msg_split[1]) / 1000000000
    end = int(msg_split[2]) / 1000000000

    if earliest_starttime == 0 or earliest_starttime > start:
        earliest_starttime = start
    if latest_endtime == 0 or latest_endtime < end:
        latest_endtime = end
    

    # start_time = float(msg_split[1])
    print("What i got before:", endtime - start_time )
    print("Total time:", latest_endtime - earliest_starttime)

# This file signals all the client threads in the different zones to start and also to collect all the total times
import socket
import time
import os
import json

clients = [
    ("15.223.68.251", 8000),
    ("18.221.100.74", 8000),
    ("13.52.254.49", 8000)
]

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(("10.0.0.166", 8000))


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

time.sleep(10)
start_time = time.time()
for i in range(len(clients)):
    startmsg = "start".encode('utf-8')
    print(clients[i])
    sock.sendto(startmsg, clients[i])

while True:
    data, addr = sock.recvfrom(1024)
    endtime = time.time()
    msg = data.decode()
    print("Received times:", msg)
    msg_split = msg.split("|")

    # start_time = float(msg_split[1])

    
    print("Total time:", endtime - start_time)

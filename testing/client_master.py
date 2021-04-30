# This file signals all the client threads in the different zones to start and also to collect all the total times
import socket
import time
import os
import json

clients = [
    ("127.0.0.1", 7000),
    ("127.0.0.1", 9000),
    ("127.0.0.1", 9500)
]

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(("127.0.0.1", 6000))


start = input("Push any key to start")
# First, send a reset signal to everyone
# reset_msg = "RESET".encode('utf-8')
# if os.path.exists("sample.json"):
#     with open("sample.json", "r") as readfile:
#         addresses = json.loads(readfile.read())
#         for addr in addresses:
#             sock.sendto(reset_msg, (addr[0], addr[1]))

time.sleep(5)


for i in range(len(clients)):
    startmsg = "start".encode('utf-8')
    print(clients[i])
    sock.sendto(startmsg, clients[i])

while True:
    data, addr = sock.recvfrom(1024)
    endtime = time.time()
    msg = data.decode()
    print("Received times:", msg)
    msg_split = msg.split(";")

    start_time = float(msg_split[1])

    
    print("Total time:", endtime - start_time)

import socket
import argparse
import threading
import random
import time
import json
import sys


def client_thread(id, num_transactions, percent, zone, primaries, times):
    client_id = id

        
    for i in range(num_transactions):
        random_number = random.random()
        new_txn = "*CLIENT_REQUEST|" + str(client_id) + "!" + str(i) + "!10|~*" 
        msgtype = ""
        if random_number < percent:
            # Do a global sync
            # print("Global")
            # print("Sending global:", new_txn, file=sys.stderr)
            primaries["0"].send(new_txn.encode('utf-8'))
            msgtype = "g"
            data = primaries["0"].recv(128)
        else:
            # print("Local")
            # Do a local trans
            # print("Sending local:", new_txn, file=sys.stderr)
            primaries[zone].send(new_txn.encode('utf-8'))
           
            msgtype = "l"
            data = primaries[zone].recv(128)

        reply = data.decode()
        # print("Received reply", reply, msgtype, file=sys.stderr)
        
        times.append(reply[1:-3])
    
    for zone in primaries:
        primaries[zone].close()


parser = argparse.ArgumentParser()
parser.add_argument("--id", "-i",  type=int, default=0)
parser.add_argument("--zone", "-z", default="0")
parser.add_argument("--numtransactions", "-t", type=int, default=10)
parser.add_argument("--percent", "-r", type=float, default=0.1)

times = []
args = parser.parse_args()


# sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
# sock.bind((args.address, args.port))

primaries = {}
with open("primaries.json", "r") as readfile:
    primary_info = json.loads(readfile.read())
    for i in range(len(primary_info)):
        # Connect with the primaries via tcp
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        s.connect( (primary_info[i]["ip"], int(primary_info[i]["port"])) )
        msg = s.recv(1024) # Receive the join message
        # print("Received:", msg, file=sys.stderr)
        primaries[ primary_info[i]["zone"]  ] = s
# start = input("input")

start = input() # Wait for start signal
print(start, file=sys.stderr)

client_thread(args.id, args.numtransactions, args.percent, args.zone, primaries, times)

times_sum = 0
starttime = 0
endtime = 0
num_success = 0
for time in times:
    components = time.split(",")
    latency = float(components[0])
    if latency > 0:
        num_success += 1
        earliest = int(components[1])
        latest = int(components[2])

        if starttime == 0 or earliest < starttime:
            starttime = earliest
        if latest > endtime:
            endtime = latest

        times_sum += latency

final_msg = str(times_sum) + "|" + str(starttime) + "|" + str(endtime) + "|" + str(num_success) + "|*"
print(final_msg)  # Write to std out


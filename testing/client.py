import socket
import argparse
import threading
import random
import time
import json


def client_thread(id, ip, port, num_transactions, percent, zone, primaries, times, start_times):
    new_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    new_sock.bind((ip, port))
    client_id = ip + ":" + str(port) + ":" + str(id)

    clientjoin_msg = "CLIENT_JOIN|" + client_id + "|" + zone + "*"
	
    with open("../addresses.txt", "r") as readfile:
        Lines = readfile.readlines()
        for line in Lines:

            ip_addr, port = line.split(" ")
            print(ip_addr, port)
            sock.sendto(clientjoin_msg.encode('utf-8'), (ip_addr, int(port)))
        
    time.sleep(10)
    num_global = 0

    num_local = 0
    clientstart = time.time()
    for i in range(num_transactions):
        random_number = random.random()
        new_txn = "CLIENT_REQUEST|" + str(client_id) + "!" + str(i) + "!10*" 
        msgtype = ""
        start = time.time()
        if random_number <= percent:
            # Do a global sync
            # print("Global")
            # print("Sending global:", new_txn)
            new_sock.sendto(new_txn.encode('utf-8'), (primaries["0"][0], int(primaries["0"][1])))
            num_global += 1
            msgtype = "g"
        else:
            # print("Local")
            # Do a local trans
            # print("Sending local:", new_txn)
            new_sock.sendto(new_txn.encode('utf-8'), (primaries[zone][0], int(primaries[zone][1])))
            num_local += 1
            msgtype = "l"
        data, addr = new_sock.recvfrom(1024)
        end = time.time()
        total_time = end - start
        reply = float(data.decode()[:-1])
        print("Received reply", reply, total_time, msgtype)
        
        if reply > 0:
            times.append(reply)
    start_times.append(clientstart)


parser = argparse.ArgumentParser()
parser.add_argument("--address", "-a", default="127.0.0.1")
parser.add_argument("--port", "-p",  type=int, default=8000)
parser.add_argument("--zone", "-z", default="0")
parser.add_argument("--numclients", "-c", type=int, default=1)
parser.add_argument("--numtransactions", "-t", type=int, default=10)
parser.add_argument("--percent", "-r", type=float, default=0.1)

starttimes = []
times = []
args = parser.parse_args()
client_id_counter = 0
num_transactions = args.numtransactions
client_threads = []
num_clients = args.numclients

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((args.address, args.port))

primaries = {}
with open("primaries.json", "r") as readfile:
    primaries = json.loads(readfile.read())

for i in range(num_clients):
    new_thread = threading.Thread(target=client_thread, args=(i, args.address, args.port + i + 1, num_transactions, args.percent, args.zone, primaries, times, starttimes))
    client_threads.append(new_thread)

# start = input("input")
data, addr = sock.recvfrom(1024) # Wait for start signal
# start = input("Press to start"

for i in range(num_clients):
    client_threads[i].start() 

for i in range(num_clients):
    client_threads[i].join()


times_sum = 0
for time in times:
    times_sum += time

final_msg = str(times_sum) + ";" + str(min(starttimes)) + ";" + str(len(times)) + "|"
sock.sendto(final_msg.encode('utf-8'), addr)
print("Done")
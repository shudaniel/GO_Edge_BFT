import argparse
import subprocess
import socket
import shlex

parser = argparse.ArgumentParser()
parser.add_argument("--address", "-a", default="127.0.0.1")
parser.add_argument("--port", "-p",  type=int, default=8000)
parser.add_argument("--id", "-i",  type=int, default=0)
parser.add_argument("--zone", "-z", default="0")
parser.add_argument("--numclients", "-c", type=int, default=1)
parser.add_argument("--numtransactions", "-t", default="10")
parser.add_argument("--percent", "-r", type=float, default=0.1)

args = parser.parse_args()

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((args.address, args.port))

client_join = "*CLIENT_JOIN|" + str(args.id) + "|" +args.zone + "|" + str(args.numclients) + "|~*" 
with open("../addresses.txt", "r") as readfile:
    Lines = readfile.readlines()
    for line in Lines:

        ip_addr, port = line.split(" ")
        print(ip_addr, port)
        sock.sendto(client_join.encode('utf-8'), (ip_addr, int(port)))

procs = []
# Spawn processes
for i in range(args.numclients):
    clientid = str(args.id + i)
    command = "python3 client.py -z " + args.zone + " -i " + clientid + " -t " + str(args.numtransactions) + " -r " + str(args.percent)
    cmd_split = shlex.split(command)
    proc = subprocess.Popen(cmd_split, stdout=subprocess.PIPE, stdin= subprocess.PIPE)
    procs.append(proc)

# Signal to send all the client_join messages
data, addr = sock.recvfrom(1024)
# Signal all the processes to start
for i in range(len(procs)):
    procs[i].stdin.write(b'start\n')
    procs[i].stdin.flush()

# Collect all of the return values
returns = []
for i in range(len(procs)):
    val, errs = procs[i].communicate()
    returns.append(val.decode("utf-8"))


times_sum = 0
starttime = 0
endtime = 0
total = 0    

# Process them
for i in range(len(returns)):
    components = returns[i].split("|")
    latency = float(components[0])
    earliest = int(components[1])
    latest = int(components[2])
    total += int(components[3])

    if starttime == 0 or earliest < starttime:
        starttime = earliest
    if latest > endtime:
        endtime = latest

    times_sum += latency

# Send back the result
final_msg = str(times_sum) + "|" + str(starttime) + "|" + str(endtime) + "|" + str(total) + "|*"
print(final_msg)
sock.sendto(final_msg.encode('utf-8'), addr)



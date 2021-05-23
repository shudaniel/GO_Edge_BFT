import argparse
import subprocess
import socket
import shlex
import json

parser = argparse.ArgumentParser()
parser.add_argument("--address", "-a", default="127.0.0.1")
parser.add_argument("--port", "-p", default="8000")
args = parser.parse_args()


# Spawn processes
while True:
    # command = "python3 client.py -z " + zone + " -i " + clientid + " -t " + str(numtransactions) + " -r " + str(percent)
    command = "go run ../client.go -a " + args.address + " -p " + args.port 
    cmd_split = shlex.split(command)
    proc = subprocess.Popen(cmd_split)
    proc.wait()



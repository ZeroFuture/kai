import os
from applescript import tell

path = os.path.dirname(os.path.abspath(__file__))

number_of_sender_slaves = 3
number_of_receiver_slaves = 3

rm_inter_port = 6000
rm_intra_port = 6001

sm_inter_port = 7000
sm_intra_port = 7001

rs_intra_ports = []
rs_inter_ports = []
for i in range(0, number_of_receiver_slaves):
    rs_intra_ports.append(9000 + (i * 2))
for i in range(0, number_of_receiver_slaves):
    rs_inter_ports.append(9001 + (i * 2))

ss_intra_ports = []
ss_inter_ports = []
for i in range(0, number_of_sender_slaves):
    ss_intra_ports.append(8000 + (i * 2))
for i in range(0, number_of_sender_slaves):
    ss_inter_ports.append(8001 + (i * 2))

start_rm = "cd {} && python3 ReceiverMaster.py {} {} result.txt".format(path, rm_inter_port, rm_intra_port)
start_sm = "cd {} && python3 SenderMaster.py {} {} localhost {} test.txt".format(path, sm_inter_port, sm_intra_port, rm_inter_port)
start_rs_commands = []
for i in range(0, number_of_receiver_slaves):
    start_rs_commands.append("cd {} && python3 ReceiverSlave.py {} {} localhost {}".format(path, rs_intra_ports[i], rs_inter_ports[i], rm_intra_port))
start_ss_commands = []
for i in range(0, number_of_sender_slaves):
    start_ss_commands.append("cd {} && python3 SenderSlave.py {} {} localhost {}".format(path, ss_intra_ports[i], ss_inter_ports[i], sm_intra_port))

commands = [start_rm, start_sm] + start_rs_commands + start_ss_commands

for command in commands:
    tell.app('Terminal', 'do script "' + command + '"')
import os
from applescript import tell

path = os.path.dirname(os.path.abspath(__file__))

rm_inter_port = 6000
rm_intra_port = 6001

sm_inter_port = 7000
sm_intra_port = 7001

rs_intra_port = 9000
rs_inter_port = 9001

ss_intra_port = 8000
ss_inter_port = 8001

start_rm = "cd {} && python3 ReceiverMaster.py {} {} result.txt".format(path, rm_inter_port, rm_intra_port)
start_sm = "cd {} && python3 SenderMaster.py {} {} localhost {} test.txt".format(path, sm_inter_port, sm_intra_port, rm_inter_port)
start_rs = "cd {} && python3 ReceiverSlave.py {} {} localhost {}".format(path, rs_intra_port, rs_inter_port, rm_intra_port)
start_ss = "cd {} && python3 SenderSlave.py {} {} localhost {}".format(path, ss_intra_port, ss_inter_port, sm_intra_port)

commands = [start_rm, start_sm, start_rs, start_ss]

for command in commands:
    tell.app('Terminal', 'do script "' + command + '"')
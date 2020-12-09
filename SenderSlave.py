import sys
import threading
import time
import os
import uuid
import pickle
from socket import *
from AtomicUtils import *
from PacketUtils import *

SYN_TIMEOUT = 3
FIN_ACK_TIMEOUT = 3

SYN_ATTEMPTS = 3
FIN_ACK_ATTEMPTS = 3

if __name__ == '__main__':

    if len(sys.argv) != 5:
        print("Usage: python3 SenderSlave.py <slave_master_port> <slave_slave_port> <sender_master_DNS_name> <sender_master_port_number>")
        sys.exit()

    try:
        slave_master_port = int(sys.argv[1])
        slave_slave_port = int(sys.argv[2])
        sender_master_port_number = int(sys.argv[4])
    except:
        print("Port number should be numerical value")
        sys.exit()
    sender_master_ip_address = sys.argv[3]

    sender_master_address = (sender_master_ip_address, sender_master_port_number)
    input_file = None

    inter_socket = socket(AF_INET, SOCK_DGRAM)
    intra_socket = socket(AF_INET, SOCK_DGRAM)
    inter_socket.bind(('localhost', slave_slave_port))
    intra_socket.bind(('localhost', slave_master_port))

    sender_master_syn_timer = None
    sender_master_fin_ack_timer = None

    slave_id = str(uuid.uuid1())
    print("Slave ID {}".format(slave_id))

    def join_cluster():
        global sender_master_syn_timer
        syn_packet = { 'packet_type': PacketType.SYN, 'slave_id': slave_id }
        intra_socket.sendto(pickle.dumps(syn_packet), sender_master_address)
        print("Joining sender cluster")
        sender_master_syn_timer = threading.Timer(SYN_TIMEOUT, syn_timeout_handler, [syn_packet, SYN_ATTEMPTS])
        sender_master_syn_timer.start()

    def sender_master_listener():
        print("Listening to sender master")
        global sender_master_fin_ack_timer
        while True:
            packet, address = intra_socket.recvfrom(PacketSize.SENDER_MASTER_TO_SLAVE)
            decoded_packet = pickle.loads(packet)
            packet_type = decoded_packet['packet_type']
            print("Packet received from sender master, packet type {}".format(PacketType.translate(packet_type)))
            if packet_type == PacketType.SYN_ACK:
                input_file = decoded_packet['input_file']
                print("Joined sender cluster, input file {}".format(input_file))
                sender_master_syn_timer.cancel()
                syn_ack_received_packet = { 'packet_type': PacketType.SYN_ACK_RECEIVED, 'slave_id': slave_id }
                intra_socket.sendto(pickle.dumps(syn_ack_received_packet), address)
            elif packet_type == PacketType.ASSIGN and input_file != None:
                # SYN_ACK should be received previous to ASSIGN, but the packet might arrive later.
                # Eventually we can still get input file because if we do not send ASSIGN_ACK, master will send it again
                # SYN_ACK will be sent again as well
                sequence_number = decoded_packet['sequence_number']
                receiver_slave_address = decoded_packet['receiver_slave_address']
                data = None
                with open(input_file) as f:
                    f.seek(sequence_number * PacketSize.DATA_SEGMENT)
                    data = f.read(PacketSize.DATA_SEGMENT)
                data_packet = { 'packet_type': PacketType.DATA, 'sequence_number': sequence_number, 'data': data }
                # No need to listen for ACK here, missing packets will be sent again. (Performance gain)
                inter_socket.sendto(pickle.dumps(data_packet), receiver_slave_address)
                print("Sending packet with sequence {} to receiver address {}".format(sequence_number, receiver_slave_address))
            elif packet_type == PacketType.PING:
                ping_ack_packet = { 'packet_type': PacketType.PING_ACK, 'slave_id': slave_id }
                intra_socket.sendto(pickle.dumps(ping_ack_packet), address)
            elif packet_type == PacketType.FIN:
                fin_ack_packet = { 'packet_type': PacketType.FIN_ACK, 'slave_id': slave_id }
                intra_socket.sendto(pickle.dumps(fin_ack_packet), address)
                sender_master_fin_ack_timer = threading.Timer(FIN_ACK_TIMEOUT, fin_ack_timeout_handler, [fin_ack_packet, FIN_ACK_ATTEMPTS])
                sender_master_fin_ack_timer.start()
            elif packet_type == PacketType.FIN_ACK_RECEIVED:
                print("Termination completed, closing")
                sender_master_fin_ack_timer.cancel()
                terminate()

    def syn_timeout_handler(syn_packet, remaining_attempts):
        global sender_master_syn_timer
        if remaining_attempts > 0:
            print("SYN timeout, retry left {}".format(remaining_attempts))
            intra_socket.sendto(pickle.dumps(syn_packet), sender_master_address)
            sender_master_syn_timer = threading.Timer(SYN_TIMEOUT, syn_timeout_handler, [syn_packet, remaining_attempts - 1])
            sender_master_syn_timer.start()

    def fin_ack_timeout_handler(fin_ack_packet, remaining_attempts):
        global sender_master_fin_ack_timer
        if remaining_attempts > 0:
            print("FIN_ACK timeout, retry left {}".format(remaining_attempts))
            intra_socket.sendto(pickle.dumps(fin_ack_packet), sender_master_address)
            sender_master_fin_ack_timer = threading.Timer(FIN_ACK_TIMEOUT, fin_ack_timeout_handler, [fin_ack_packet, remaining_attempts - 1])
            sender_master_fin_ack_timer.start()
    
    def terminate():
        inter_socket.close()
        intra_socket.close()
        os._exit(1)

    join_cluster()
    sender_master_listen_thread = threading.Thread(target=sender_master_listener)
    sender_master_listen_thread.start()
        
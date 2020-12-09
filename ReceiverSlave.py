import sys
import threading
import time
import os
import uuid
import pickle
from socket import *
from AtomicUtils import *
from PacketUtils import *

# receiver <receiver_master_DNS_name> <receiver_master_port_number>

SYN_ACK_TIMEOUT = 3
FIN_ACK_TIMEOUT = 3
PACKET_RECEIVED_TIMEOUT = 3

SYN_ACK_ATTEMPTS = 3
FIN_ACK_ATTEMPTS = 3
PACKET_RECEIVED_ATTEMPTS = 3

if __name__ == '__main__':

    if len(sys.argv) != 4:
        print("Usage: python3 ReceiverSlave.py <slave_slave_port> <receiver_master_DNS_name> <receiver_master_port_number>")
        sys.exit()
    
    try:
        slave_slave_port = int(sys.argv[1])
        receiver_master_port_number = int(sys.argv[3])
    except:
        print("Port number should be numerical value")
        sys.exit()
    receiver_master_ip_address = sys.argv[2]

    receiver_master_address = (receiver_master_ip_address, receiver_master_port_number)
    output_file = None
    inter_socket = socket(AF_INET, SOCK_DGRAM)
    intra_socket = socket(AF_INET, SOCK_DGRAM)
    receiver_master_syn_ack_timer = None
    receiver_master_fin_ack_timer = None
    receiver_master_packet_received_timer = None

    slave_id = str(uuid.uuid1())

    def join_cluster():
        syn_packet = { 'packet_type': PacketType.SYN, 'slave_id': slave_id }
        intra_socket.sendto(pickle.dumps(syn_packet), receiver_master_address)
        receiver_master_syn_ack_timer = threading.Timer(SYN_ACK_TIMEOUT, syn_timeout_handler, [syn_packet, SYN_ACK_ATTEMPTS])
        receiver_master_syn_ack_timer.start()

    def sender_slave_listener():
        while True:
            packet, address = inter_socket.recvfrom(PacketSize.SENDER_SLAVE_TO_RECEIVER)
            decoded_packet = pickle.loads(packet)
            packet_type = decoded_packet['packet_type']
            if packet_type == PacketType.DATA and output_file is not None:
                packet_received_packet = { 'packet_type': PacketType.PACKET_RECEIVED, 'slave_id': slave_id, 'sequence_number': sequence_number }
                intra_socket.sendto(pickle.dumps(packet_received_packet), receiver_master_address)
                receiver_master_packet_received_timer = threading.Timer(PACKET_RECEIVED_TIMEOUT, packet_received_timeout_handler, [packet_received_packet, PACKET_RECEIVED_ATTEMPTS])
                receiver_master_packet_received_timer.start()
                sequence_number = decoded_packet['sequence_number']
                data = decoded_packet['data']
                with open(output_file, 'wb') as f:
                    f.seek(sequence_number * PacketSize.DATA_SEGMENT)
                    f.write(data)

    def receiver_master_listener():
        while True:
            packet, address = intra_socket.recvfrom(PacketSize.RECEIVER_MASTER_TO_SLAVE)
            decoded_packet = pickle.loads(packet)
            packet_type = decoded_packet['packet_type']
            if packet_type == PacketType.SYN_ACK:
                output_file = decoded_packet['output_file']
                receiver_master_syn_ack_timer.cancel()
                syn_ack_received_packet = { 'packet_type': PacketType.SYN_ACK_RECEIVED, 'slave_id': slave_id }
                intra_socket.sendto(pickle.dumps(syn_ack_received_packet), address)
            elif packet_type == PacketType.FIN: 
                fin_ack_packet = { 'packet_type': PacketType.FIN_ACK, 'slave_id': slave_id }
                intra_socket.sendto(pickle.dumps(fin_ack_packet), address)
                receiver_master_fin_ack_timer = threading.Timer(FIN_ACK_TIMEOUT, fin_ack_timeout_handler, [fin_ack_packet, FIN_ACK_ATTEMPTS])
                receiver_master_fin_ack_timer.start()
            elif packet_type == PacketType.FIN_ACK_RECEIVED:
                receiver_master_fin_ack_timer.cancel()
                terminate()

    def syn_timeout_handler(syn_packet, remaining_attempts):
        if remaining_attempts > 0:
            intra_socket.sendto(pickle.dumps(syn_packet), receiver_master_address)
            receiver_master_syn_ack_timer = threading.Timer(SYN_ACK_TIMEOUT, syn_timeout_handler, [syn_packet, remaining_attempts - 1])
            receiver_master_syn_ack_timer.start()

    def packet_received_timeout_handler(packet_received_packet, remaining_attempts):
        if remaining_attempts > 0:
            intra_socket.sendto(pickle.dumps(packet_received_packet), receiver_master_address)
            receiver_master_packet_received_timer = threading.Timer(PACKET_RECEIVED_TIMEOUT, packet_received_timeout_handler, [packet_received_packet, remaining_attempts - 1])
            receiver_master_packet_received_timer.start()
    
    def fin_ack_timeout_handler(fin_ack_packet, remaining_attempts):
        if remaining_attempts > 0:
            intra_socket.sendto(pickle.dumps(fin_ack_packet), receiver_master_address)
            receiver_master_fin_ack_timer = threading.Timer(FIN_ACK_TIMEOUT, fin_ack_timeout_handler, [fin_ack_packet, remaining_attempts - 1])
            receiver_master_fin_ack_timer.start()

    def terminate():
        inter_socket.close()
        intra_socket.close()
        os._exit(1)

    join_cluster()

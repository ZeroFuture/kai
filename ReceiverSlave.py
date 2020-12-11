import sys
import threading
import time
import os
import uuid
import pickle
import random
from socket import *
from AtomicUtils import *
from PacketUtils import *

SYN_ACK_TIMEOUT = 3
FIN_ACK_TIMEOUT = 3

SYN_ACK_ATTEMPTS = 3
FIN_ACK_ATTEMPTS = 3

TERMINATION_SCHEDULER_DELAY = 5

MAX_NEW_RECEIVED_PACKETS_LEN = 1000

RANDOM_DROP_PROB = 0.0

if __name__ == '__main__':

    if len(sys.argv) != 5:
        print("Usage: python3 ReceiverSlave.py <intra_port> <inter_port> <receiver_master_DNS_name> <receiver_master_port>")
        sys.exit()
    
    try:
        slave_master_port = int(sys.argv[1])
        slave_slave_port = int(sys.argv[2])
        receiver_master_port_number = int(sys.argv[4])
    except:
        print("Port number should be numerical value")
        sys.exit()
    receiver_master_ip_address = sys.argv[3]

    receiver_master_address = (receiver_master_ip_address, receiver_master_port_number)
    slave_file = None

    inter_socket = socket(AF_INET, SOCK_DGRAM)
    intra_socket = socket(AF_INET, SOCK_DGRAM)
    inter_socket.bind(('localhost', slave_slave_port))
    intra_socket.bind(('localhost', slave_master_port))

    receiver_master_syn_ack_timer = None
    receiver_master_fin_ack_timer = None
    received_sequences = []
    received_sequence_set = set()

    slave_id = str(uuid.uuid1())
    print("Slave ID {}".format(slave_id))

    is_terminated = AtomicBoolean(False)

    def join_cluster():
        global receiver_master_syn_ack_timer
        syn_packet = { 'packet_type': PacketType.SYN, 'slave_id': slave_id, 'inter_port': ('localhost', slave_slave_port) }
        receiver_master_syn_ack_timer = threading.Timer(SYN_ACK_TIMEOUT, syn_timeout_handler, [syn_packet, SYN_ACK_ATTEMPTS])
        receiver_master_syn_ack_timer.start()
        intra_socket.sendto(pickle.dumps(syn_packet), receiver_master_address)
        print("Joining receiver cluster")

    def sender_slave_listener():
        print("Listening to sender slave")
        while not is_terminated.get():
            packet, address = inter_socket.recvfrom(PacketSize.SENDER_SLAVE_TO_RECEIVER)
            if random.uniform(0, 1) < RANDOM_DROP_PROB:
                continue
            decoded_packet = pickle.loads(packet)
            packet_type = decoded_packet['packet_type']
            print("Packet received from sender slave, packet type {}".format(PacketType.translate(packet_type)))
            if packet_type == PacketType.DATA and slave_file != None:
                sequence_number = decoded_packet['sequence_number']
                # we may receive duplicate packets, simply ignore
                if not sequence_number in received_sequence_set:
                    received_sequences.append(sequence_number)
                    received_sequence_set.add(sequence_number)
                    data = decoded_packet['data']
                    # offset the last packet which may not be in the size of PacketSize.DATA_SEGMENT
                    if len(data.encode('utf-8')) < PacketSize.DATA_SEGMENT:
                        data += (PacketSize.DATA_SEGMENT - len(data.encode('utf-8'))) * " " 
                    with open(slave_file, 'a') as f:
                        f.write(data)
                    print("Data with sequence {} received and write to temp file".format(sequence_number))
        inter_socket.close()

    def receiver_master_listener():
        global receiver_master_fin_ack_timer
        global slave_file
        print("Listening to receiver master")
        while not is_terminated.get():
            packet, address = intra_socket.recvfrom(PacketSize.RECEIVER_MASTER_TO_SLAVE)
            if random.uniform(0, 1) < RANDOM_DROP_PROB:
                continue
            decoded_packet = pickle.loads(packet)
            packet_type = decoded_packet['packet_type']
            print("Packet received from receiver master, packet type {}".format(PacketType.translate(packet_type)))
            if packet_type == PacketType.SYN_ACK:
                output_file = decoded_packet['output_file']
                slave_file = output_file.split('.')[0] + '_' + slave_id + '.' + output_file.split('.')[1]
                print("Joined receiver cluster, output file {}".format(output_file))
                receiver_master_syn_ack_timer.cancel()
                syn_ack_received_packet = { 'packet_type': PacketType.SYN_ACK_RECEIVED, 'slave_id': slave_id }
                intra_socket.sendto(pickle.dumps(syn_ack_received_packet), address)
            elif packet_type == PacketType.PING:
                last_received_sequences_size = decoded_packet['last_received_sequences_size']
                new_received_sequences = received_sequences[last_received_sequences_size:last_received_sequences_size + MAX_NEW_RECEIVED_PACKETS_LEN]
                print("Newly received sequences since last PING_ACK sent {}".format(new_received_sequences))
                ping_ack_packet = { 'packet_type': PacketType.PING_ACK, 'slave_id': slave_id, 'new_received_sequences': new_received_sequences }
                intra_socket.sendto(pickle.dumps(ping_ack_packet), address)
            elif packet_type == PacketType.FIN:
                fin_ack_packet = { 'packet_type': PacketType.FIN_ACK, 'slave_id': slave_id }
                receiver_master_fin_ack_timer = threading.Timer(FIN_ACK_TIMEOUT, fin_ack_timeout_handler, [fin_ack_packet, FIN_ACK_ATTEMPTS])
                receiver_master_fin_ack_timer.start()
                intra_socket.sendto(pickle.dumps(fin_ack_packet), address)
            elif packet_type == PacketType.FIN_ACK_RECEIVED:
                print("Termination completed, closing")
                receiver_master_fin_ack_timer.cancel()
                is_terminated.set(True)
        intra_socket.close()

    def syn_timeout_handler(syn_packet, remaining_attempts):
        if is_terminated.get():
            return
        global receiver_master_syn_ack_timer
        if remaining_attempts > 0:
            print("SYN timeout, attempts left {}".format(remaining_attempts))
            receiver_master_syn_ack_timer = threading.Timer(SYN_ACK_TIMEOUT, syn_timeout_handler, [syn_packet, remaining_attempts - 1])
            receiver_master_syn_ack_timer.start()
            intra_socket.sendto(pickle.dumps(syn_packet), receiver_master_address)
    
    def fin_ack_timeout_handler(fin_ack_packet, remaining_attempts):
        if is_terminated.get():
            return
        global receiver_master_fin_ack_timer
        if remaining_attempts > 0:
            print("FIN_ACK timeout, attempts left {}".format(remaining_attempts))
            receiver_master_fin_ack_timer = threading.Timer(FIN_ACK_TIMEOUT, fin_ack_timeout_handler, [fin_ack_packet, remaining_attempts - 1])
            receiver_master_fin_ack_timer.start()
            intra_socket.sendto(pickle.dumps(fin_ack_packet), receiver_master_address)
    
    def termination_schedule_event():
        while True:
            if is_terminated.get():
                os._exit(0)
            time.sleep(TERMINATION_SCHEDULER_DELAY)

    join_cluster()
    receiver_master_listen_thread = threading.Thread(target=receiver_master_listener)
    sender_slave_listen_thread = threading.Thread(target=sender_slave_listener)
    termination_schedule_thread = threading.Thread(target=termination_schedule_event)
    receiver_master_listen_thread.start()
    sender_slave_listen_thread.start()
    termination_schedule_thread.start()

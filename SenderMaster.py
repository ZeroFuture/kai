import sys
import threading
import random
import os
import pickle
import math
from socket import *
from AtomicUtils import *
from PacketUtils import *

SYN_TIMEOUT = 3
SYN_ACK_TIMEOUT = 3
FIN_TIMEOUT = 3
FIN_ACK_TIMEOUT = 3

SYN_ATTEMPTS = 3
SYN_ACK_ATTEMPTS = 3
FIN_ATTEMPTS = 3
FIN_ACK_ATTEMPTS = 3

if __name__ == '__main__':
    
    if len(sys.argv) != 6:
        print("Usage: python3 SenderMaster.py <master_master_port> <master_slave_port> <receiver_master_ip> <receiver_master_port> <input_file>")
        sys.exit()

    try:
        master_master_port = int(sys.argv[1])
        master_slave_port = int(sys.argv[2])
        receiver_master_port = int(sys.argv[4])
    except:
        print("Port numbers and max window size should be numerical value")
        sys.exit()

    receiver_master_ip = sys.argv[3]
    input_file = sys.argv[5]
    
    inter_socket = socket(AF_INET, SOCK_DGRAM)
    intra_socket = socket(AF_INET, SOCK_DGRAM)
    inter_socket.bind(('localhost', master_master_port))
    intra_socket.bind(('localhost', master_slave_port))

    receiver_master_address = (receiver_master_ip, receiver_master_port)
    slave_addresses = {}
    receiver_slave_addresses = []
    slave_syn_ack_timers = {}
    slave_fin_timers = {}
    receiver_master_syn_timer = None
    receiver_master_fin_ack_timer = None
    
    file_size = os.stat(input_file).st_size
    number_of_segments = math.ceil(file_size / PacketSize.DATA_SEGMENT)
    is_receiver_terminated = AtomicBoolean(False)

    def connect_to_receiver_master():
        global receiver_master_syn_timer
        # three-way handshake to establish connection with receiver master
        print("Connecting to receiver master")
        syn_packet = { 'packet_type': PacketType.SYN, 'number_of_segments': number_of_segments }
        inter_socket.sendto(pickle.dumps(syn_packet), receiver_master_address)
        receiver_master_syn_timer = threading.Timer(SYN_TIMEOUT, syn_timeout_handler, [syn_packet, SYN_ATTEMPTS])
        receiver_master_syn_timer.start()

    def assign_sequence_numbers(sequence_numbers):
        for sequence_number in sequence_numbers:
            is_packet_assigned = False
            while not is_packet_assigned:
                if len(slave_addresses) > 0 and len(receiver_slave_addresses) > 0:
                    # randomly choose a slave to assign this sequence_number
                    slave_id = random.choice(list(slave_addresses))
                    slave_address = slave_addresses[slave_id]
                    receiver_slave_address = random.choice(receiver_slave_addresses)
                    print("Assigning packet sequence {} to slave {} with receiver slave address {}".format(sequence_number, slave_id, receiver_slave_address))
                    assign_packet = { 'packet_type': PacketType.ASSIGN, 'sequence_number': sequence_number, 'receiver_slave_address': receiver_slave_address }
                    intra_socket.sendto(pickle.dumps(assign_packet), slave_address)
                    is_packet_assigned = True

    def sender_slave_listener():
        print("Listening to sender slaves")
        while True:
            packet, address = intra_socket.recvfrom(PacketSize.SENDER_SLAVE_TO_MASTER)
            decoded_packet = pickle.loads(packet)
            slave_id = decoded_packet['slave_id']
            packet_type = decoded_packet['packet_type']
            print("Packet received from sender slave {}, packet type {}".format(slave_id, PacketType.translate(packet_type)))
            if packet_type == PacketType.SYN:
                slave_addresses[slave_id] = address
                syn_ack_packet = { 'packet_type': PacketType.SYN_ACK, 'input_file': input_file }
                intra_socket.sendto(pickle.dumps(syn_ack_packet), address)
                print("slave {} joined cluster".format(slave_id))
                if not slave_id in slave_syn_ack_timers:
                    slave_syn_ack_timers[slave_id] = threading.Timer(SYN_ACK_TIMEOUT, syn_ack_timeout_handler, [slave_id, syn_ack_packet, address, SYN_ACK_ATTEMPTS])
                    slave_syn_ack_timers[slave_id].start()
            elif packet_type == PacketType.SYN_ACK_RECEIVED:
                slave_syn_ack_timers[slave_id].cancel()
            elif packet_type == PacketType.FIN_ACK:
                slave_fin_timers[slave_id].cancel()
                fin_ack_received_packet = { 'packet_type': PacketType.FIN_ACK_RECEIVED }
                intra_socket.sendto(pickle.dumps(fin_ack_received_packet), address)
                if slave_id in slave_addresses:
                    # finished termination with this slave, removing it from the list
                    slave_addresses.pop(slave_id)
                    if len(slave_addresses) == 0 and is_receiver_terminated.get():
                        print("All sender slaves and receiver master are terminated, closing")
                        # all slaves and receiver master is terminated, terminate sender master here
                        terminate()

    def receiver_master_listener():
        global receiver_slave_addresses
        global receiver_master_fin_ack_timer
        print("Listening to receiver master")
        while True:
            packet, address = inter_socket.recvfrom(PacketSize.RECEIVER_MASTER_TO_SENDER)
            decoded_packet = pickle.loads(packet)
            packet_type = decoded_packet['packet_type']
            print("Packet received from receiver master, packet type {}".format(PacketType.translate(packet_type)))
            if packet_type == PacketType.RECEIVER_ADDRESSES:
                receiver_slave_addresses = decoded_packet['slave_addresses']
                print("Updated receiver slave addresses to {}".format(receiver_slave_addresses))
                receiver_addresses_ack_packet = { 'packet_type': PacketType.RECEIVER_ADDRESSES_ACK }
                inter_socket.sendto(pickle.dumps(receiver_addresses_ack_packet), address)
            elif packet_type == PacketType.SYN_ACK:
                print("Receiver master connected")
                receiver_master_syn_timer.cancel()
                receiver_slave_addresses = decoded_packet['slave_addresses']
                print("Current receiver slave addresses are {}".format(receiver_slave_addresses))
                syn_ack_received_packet = { 'packet_type': PacketType.SYN_ACK_RECEIVED }
                inter_socket.sendto(pickle.dumps(syn_ack_received_packet), address)
            elif packet_type == PacketType.ACK:
                ack_received_packet = { 'packet_type': PacketType.ACK_RECEIVED }
                inter_socket.sendto(pickle.dumps(ack_received_packet), address)
                missing_sequence_numbers = decoded_packet['missing_sequence_numbers']
                print("Cumulated ACK received, missing sequence numbers {}".format(missing_sequence_numbers))
                if len(missing_sequence_numbers) > 0:
                    # re-assign missing sequence numbers
                    assign_sequence_numbers(missing_sequence_numbers)
            elif packet_type == PacketType.FIN:
                print("Close connection requested from receiver master")
                # four-way termination
                # merge FIN and ACK to one packet because the sender has no more data to sent at this point
                fin_ack_packet = { 'packet_type': PacketType.FIN_ACK }
                inter_socket.sendto(pickle.dumps(fin_ack_packet), address)
                receiver_master_fin_ack_timer = threading.Timer(FIN_ACK_TIMEOUT, fin_ack_timeout_handler, [fin_ack_packet, FIN_ACK_ATTEMPTS])
                receiver_master_fin_ack_timer.start()
                # asking all sender slaves to terminate
                fin_packet = { 'packet_type': PacketType.FIN }
                for slave_id, slave_address in slave_addresses.items():
                    intra_socket.sendto(pickle.dumps(fin_packet), slave_address)
                    print("Noticing slave {} to terminate".format(slave_id))
                    slave_fin_timers[slave_id] = threading.Timer(FIN_TIMEOUT, fin_timeout_handler, [slave_id, fin_packet, slave_address, FIN_ATTEMPTS])
                    slave_fin_timers[slave_id].start()
            elif packet_type == PacketType.FIN_ACK_RECEIVED:
                # receiver master successfully terminated
                print("Receiver master successfully terminated")
                receiver_master_fin_ack_timer.cancel()
                is_receiver_terminated.set(True)
                if len(slave_addresses) == 0:
                    terminate()

    def syn_timeout_handler(syn_packet, remaining_attempts):
        global receiver_master_syn_timer
        if remaining_attempts > 0:
            print("SYN timeout, retry left {}".format(remaining_attempts))
            inter_socket.sendto(pickle.dumps(syn_packet), receiver_master_address)
            receiver_master_syn_timer = threading.Timer(SYN_TIMEOUT, syn_timeout_handler, [syn_packet, remaining_attempts - 1])
            receiver_master_syn_timer.start()

    def syn_ack_timeout_handler(slave_id, syn_ack_packet, slave_address, remaining_attempts):
        # recursively resend the syn_ack packet until no remaining attempts left we we assume that slave is down
        if remaining_attempts > 0:
            print("SYN_ACK timeout on slave {}, retry left {}".format(slave_id, remaining_attempts))
            intra_socket.sendto(pickle.dumps(syn_ack_packet), slave_address)
            slave_syn_ack_timers[slave_id] = threading.Timer(SYN_ACK_TIMEOUT, syn_ack_timeout_handler, [slave_id, syn_ack_packet, slave_address, remaining_attempts - 1])
            slave_syn_ack_timers[slave_id].start()

    def fin_timeout_handler(slave_id, fin_packet, slave_address, remaining_attempts):
        # recursively resend the fin packet until no remaining attempts left we we assume that slave is down
        if remaining_attempts > 0:
            print("FIN timeout on slave {}, retry left {}".format(slave_id, remaining_attempts))
            intra_socket.sendto(pickle.dumps(fin_packet), slave_address)
            slave_fin_timers[slave_id] = threading.Timer(FIN_TIMEOUT, fin_timeout_handler, [slave_id, fin_packet, slave_address, remaining_attempts - 1])
            slave_fin_timers[slave_id].start()

    def fin_ack_timeout_handler(fin_ack_packet, remaining_attempts):
        global receiver_master_fin_ack_timer
        # recursively resend the fin_ack packet until no remaining attempts left we we assume that receiver master is down
        if remaining_attempts > 0:
            print("FIN_ACK timeout, retry left {}".format(remaining_attempts))
            inter_socket.sendto(pickle.dumps(fin_ack_packet), receiver_master_address)
            receiver_master_fin_ack_timer = threading.Timer(FIN_ACK_TIMEOUT, fin_ack_timeout_handler, [fin_ack_packet, remaining_attempts - 1])
            receiver_master_fin_ack_timer.start()

    def terminate():
        # all slaves and receiver master is terminated, terminate sender master here
        inter_socket.close()
        intra_socket.close()
        os._exit(1)

    connect_to_receiver_master()
    slaves_listener_thread = threading.Thread(target=sender_slave_listener)
    receiver_master_listener_thread = threading.Thread(target=receiver_master_listener)
    slaves_listener_thread.start()
    receiver_master_listener_thread.start()
    assign_sequence_numbers(list(range(0, number_of_segments)))

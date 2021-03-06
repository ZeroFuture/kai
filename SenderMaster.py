import sys
import threading
import random
import os
import pickle
import math
import time
import copy
from socket import *
from AtomicUtils import *
from PacketUtils import *

SYN_TIMEOUT = 3
SYN_ACK_TIMEOUT = 3
FIN_TIMEOUT = 3
FIN_ACK_TIMEOUT = 3
PING_TIMEOUT = 1

SYN_ATTEMPTS = 3
SYN_ACK_ATTEMPTS = 3
FIN_ATTEMPTS = 3
FIN_ACK_ATTEMPTS = 3
PING_ATTEMPTS = 5

PING_SCHEDULER_DELAY = 1
WAIT_SLAVES_DELAY = 1
TERMINATION_SCHEDULER_DELAY = 5

RANDOM_DROP_PROB = 0.0

if __name__ == '__main__':
    
    if len(sys.argv) != 6:
        print("Usage: python3 SenderMaster.py <inter_port> <intra_port> <receiver_master_ip> <receiver_master_port> <input_file>")
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
    slave_ping_timers = {}
    slave_ping_remaining_attempts = {}
    receiver_master_syn_timer = None
    receiver_master_fin_ack_timer = None
    
    file_size = os.stat(input_file).st_size
    number_of_segments = math.ceil(file_size / PacketSize.DATA_SEGMENT)
    is_receiver_terminated = AtomicBoolean(False)

    is_terminated = AtomicBoolean(False)

    def connect_to_receiver_master():
        global receiver_master_syn_timer
        # three-way handshake to establish connection with receiver master
        print("Connecting to receiver master")
        syn_packet = { 'packet_type': PacketType.SYN, 'number_of_segments': number_of_segments }
        receiver_master_syn_timer = threading.Timer(SYN_TIMEOUT, syn_timeout_handler, [syn_packet, SYN_ATTEMPTS])
        receiver_master_syn_timer.start()
        inter_socket.sendto(pickle.dumps(syn_packet), receiver_master_address)

    def assign_sequence_numbers(sequence_numbers):
        for sequence_number in sequence_numbers:
            is_packet_assigned = False
            print("Assigning packet sequence {}".format(sequence_number))
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
                else:
                    print("Waiting for receiver slaves and sender slaves to join")
                    time.sleep(WAIT_SLAVES_DELAY)

    def sender_slave_listener():
        print("Listening to sender slaves")
        while not is_terminated.get():
            packet, address = intra_socket.recvfrom(PacketSize.SENDER_SLAVE_TO_MASTER)
            if random.uniform(0, 1) < RANDOM_DROP_PROB:
                continue
            decoded_packet = pickle.loads(packet)
            slave_id = decoded_packet['slave_id']
            packet_type = decoded_packet['packet_type']
            print("Packet received from sender slave {}, packet type {}".format(slave_id, PacketType.translate(packet_type)))
            if packet_type == PacketType.SYN:
                slave_addresses[slave_id] = address
                syn_ack_packet = { 'packet_type': PacketType.SYN_ACK, 'input_file': input_file }
                if not slave_id in slave_syn_ack_timers:
                    slave_syn_ack_timers[slave_id] = threading.Timer(SYN_ACK_TIMEOUT, syn_ack_timeout_handler, [slave_id, syn_ack_packet, address, SYN_ACK_ATTEMPTS])
                    slave_syn_ack_timers[slave_id].start()
                intra_socket.sendto(pickle.dumps(syn_ack_packet), address)
                print("slave {} joined cluster".format(slave_id))
            elif packet_type == PacketType.SYN_ACK_RECEIVED:
                slave_syn_ack_timers[slave_id].cancel()
            elif packet_type == PacketType.PING_ACK and slave_id in slave_ping_timers:
                slave_ping_remaining_attempts[slave_id] = PING_ATTEMPTS
                slave_ping_timers[slave_id].cancel()
            elif packet_type == PacketType.FIN_ACK:
                slave_fin_timers[slave_id].cancel()
                slave_ping_timers[slave_id].cancel()
                fin_ack_received_packet = { 'packet_type': PacketType.FIN_ACK_RECEIVED }
                intra_socket.sendto(pickle.dumps(fin_ack_received_packet), address)
                if slave_id in slave_fin_timers:
                    slave_fin_timers[slave_id].cancel()
                if slave_id in slave_ping_timers:
                    slave_ping_timers[slave_id].cancel()
                if slave_id in slave_addresses:
                    print("Slave {} terminated".format(slave_id))
                    # finished termination with this slave, removing it from the list
                    slave_addresses.pop(slave_id)
                    if len(slave_addresses) == 0 and is_receiver_terminated.get():
                        print("All sender slaves and receiver master are terminated, closing")
                        # all slaves and receiver master is terminated, terminate sender master here
                        is_terminated.set(True)
        intra_socket.close()

    def receiver_master_listener():
        global receiver_slave_addresses
        global receiver_master_fin_ack_timer
        print("Listening to receiver master")
        while not is_terminated.get():
            packet, address = inter_socket.recvfrom(PacketSize.RECEIVER_MASTER_TO_SENDER)
            if random.uniform(0, 1) < RANDOM_DROP_PROB:
                continue
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
                receiver_master_fin_ack_timer = threading.Timer(FIN_ACK_TIMEOUT, fin_ack_timeout_handler, [fin_ack_packet, FIN_ACK_ATTEMPTS])
                receiver_master_fin_ack_timer.start()
                inter_socket.sendto(pickle.dumps(fin_ack_packet), address)
                # asking all sender slaves to terminate
                fin_packet = { 'packet_type': PacketType.FIN }
                slave_addresses_copy = copy.deepcopy(slave_addresses)
                for slave_id, slave_address in slave_addresses_copy.items():
                    slave_fin_timers[slave_id] = threading.Timer(FIN_TIMEOUT, fin_timeout_handler, [slave_id, fin_packet, slave_address, FIN_ATTEMPTS])
                    slave_fin_timers[slave_id].start()
                    intra_socket.sendto(pickle.dumps(fin_packet), slave_address)
                    print("Noticing slave {} to terminate".format(slave_id))
            elif packet_type == PacketType.FIN_ACK_RECEIVED:
                # receiver master successfully terminated
                print("Receiver master successfully terminated")
                receiver_master_fin_ack_timer.cancel()
                is_receiver_terminated.set(True)
                if len(slave_addresses) == 0:
                    is_terminated.set(True)
        inter_socket.close()

    def syn_timeout_handler(syn_packet, remaining_attempts):
        if is_terminated.get():
            return
        global receiver_master_syn_timer
        if remaining_attempts > 0:
            print("SYN timeout, retry left {}".format(remaining_attempts))
            receiver_master_syn_timer = threading.Timer(SYN_TIMEOUT, syn_timeout_handler, [syn_packet, remaining_attempts - 1])
            receiver_master_syn_timer.start()
            inter_socket.sendto(pickle.dumps(syn_packet), receiver_master_address)

    def syn_ack_timeout_handler(slave_id, syn_ack_packet, slave_address, remaining_attempts):
        if is_terminated.get():
            return
        # recursively resend the syn_ack packet until no remaining attempts left we we assume that slave is down
        if remaining_attempts > 0:
            print("SYN_ACK timeout on slave {}, retry left {}".format(slave_id, remaining_attempts))
            slave_syn_ack_timers[slave_id] = threading.Timer(SYN_ACK_TIMEOUT, syn_ack_timeout_handler, [slave_id, syn_ack_packet, slave_address, remaining_attempts - 1])
            slave_syn_ack_timers[slave_id].start()
            intra_socket.sendto(pickle.dumps(syn_ack_packet), slave_address)

    def fin_timeout_handler(slave_id, fin_packet, slave_address, remaining_attempts):
        if is_terminated.get():
            return
        # recursively resend the fin packet until no remaining attempts left we we assume that slave is down
        if remaining_attempts > 0:
            print("FIN timeout on slave {}, retry left {}".format(slave_id, remaining_attempts))
            slave_fin_timers[slave_id] = threading.Timer(FIN_TIMEOUT, fin_timeout_handler, [slave_id, fin_packet, slave_address, remaining_attempts - 1])
            slave_fin_timers[slave_id].start()
            intra_socket.sendto(pickle.dumps(fin_packet), slave_address)
        else:
            # slave down, removing from the list
            print("Slave {} down, FIN reached max attempts".format(slave_id))
            slave_addresses.pop(slave_id)
            if slave_id in slave_ping_timers:
                slave_ping_timers[slave_id].cancel()
                slave_ping_timers.pop(slave_id)
            if len(slave_addresses) == 0 and is_receiver_terminated.get():
                    is_terminated.set(True)

    def fin_ack_timeout_handler(fin_ack_packet, remaining_attempts):
        if is_terminated.get():
            return
        global receiver_master_fin_ack_timer
        # recursively resend the fin_ack packet until no remaining attempts left we we assume that receiver master is down
        if remaining_attempts > 0:
            print("FIN_ACK timeout, retry left {}".format(remaining_attempts))
            receiver_master_fin_ack_timer = threading.Timer(FIN_ACK_TIMEOUT, fin_ack_timeout_handler, [fin_ack_packet, remaining_attempts - 1])
            receiver_master_fin_ack_timer.start()
            inter_socket.sendto(pickle.dumps(fin_ack_packet), receiver_master_address)

    def ping_timeout_handler(slave_id, ping_packet, slave_address):
        if is_terminated.get() or not slave_id in slave_addresses:
            return
        slave_ping_remaining_attempts[slave_id] -= 1
        remaining_attempts = slave_ping_remaining_attempts[slave_id]
        if remaining_attempts > 0:
            print("PING timeout, retry left {}".format(remaining_attempts))
            slave_ping_timers[slave_id] = threading.Timer(PING_TIMEOUT, ping_timeout_handler, [slave_id, ping_packet, slave_address])
            slave_ping_timers[slave_id].start()
            intra_socket.sendto(pickle.dumps(ping_packet), slave_address)
        else:
            # slave down, removing from the list
            print("Slave {} down, health check reached max attempts".format(slave_id))
            slave_addresses.pop(slave_id)
            if slave_id in slave_ping_timers:
                slave_ping_timers[slave_id].cancel()
                slave_ping_timers.pop(slave_id)
            if len(slave_addresses) == 0 and is_receiver_terminated.get():
                    is_terminated.set(True)
            

    def ping_schedule_event():
        while not is_terminated.get():
            slave_addresses_copy = copy.deepcopy(slave_addresses)
            for slave_id, slave_address in slave_addresses_copy.items():
                ping_packet = { 'packet_type': PacketType.PING }
                # ignore old timer
                if slave_id in slave_ping_timers:
                    slave_ping_timers[slave_id].cancel()
                slave_ping_timers[slave_id] = threading.Timer(PING_TIMEOUT, ping_timeout_handler, [slave_id, ping_packet, slave_address])
                slave_ping_timers[slave_id].start()
                intra_socket.sendto(pickle.dumps(ping_packet), slave_address)
            time.sleep(PING_SCHEDULER_DELAY)
    
    def termination_schedule_event():
        while True:
            if is_terminated.get():
                os._exit(0)
            time.sleep(TERMINATION_SCHEDULER_DELAY)

    connect_to_receiver_master()
    slaves_listener_thread = threading.Thread(target=sender_slave_listener)
    receiver_master_listener_thread = threading.Thread(target=receiver_master_listener)
    ping_schedule_thread = threading.Thread(target=ping_schedule_event)
    termination_schedule_thread = threading.Thread(target=termination_schedule_event)
    slaves_listener_thread.start()
    receiver_master_listener_thread.start()
    ping_schedule_thread.start()
    termination_schedule_thread.start()
    assign_sequence_numbers(list(range(0, number_of_segments)))
import sys
import threading
import random
import os
import sched
import time
import pickle
from socket import *
from AtomicUtils import *
from PacketUtils import *

SENDER_SYN_ACK_TIMEOUT = 3
SLAVE_SYN_ACK_TIMEOUT = 3
SLAVE_FIN_TIMEOUT = 3
SENDER_FIN_TIMEOUT = 3
RECEIVER_ADDRESSES_TIMEOUT = 3
ACK_TIMEOUT = 3

SENDER_SYN_ACK_ATTEMPTS = 3
SLAVE_SYN_ACK_ATTEMPTS = 3
SLAVE_FIN_ATTEMPTS = 3
SENDER_FIN_ATTEMPTS = 3
RECEIVER_ADDRESSES_ATTEMPTS = 3
ACK_ATTEMPTS = 3

ACK_SCHEDULER_DELAY = 3

if __name__ == '__main__':

    if len(sys.argv) != 4:
        print("Usage: python3 ReceiverMaster.py <master_master_port> <master_slave_port> <output_file>")
        sys.exit()
    
    try:
        master_master_port = int(sys.argv[1])
        master_slave_port = int(sys.argv[2])
    except:
        print("Port numbers and max window size should be numerical value")
        sys.exit()
    
    output_file = sys.argv[3]

    inter_socket = socket(AF_INET, SOCK_DGRAM)
    intra_socket = socket(AF_INET, SOCK_DGRAM)
    inter_socket.bind(('localhost', master_master_port))
    intra_socket.bind(('localhost', master_slave_port))

    slave_addresses = {}
    sender_master_address = None
    number_of_segments = None

    slave_syn_ack_timers = {}
    slave_fin_timers = {}
    sender_master_syn_ack_timer = None
    sender_master_receiver_addresses_timer = None
    sender_master_ack_timer = None
    sender_master_fin_timer = None
    ack_scheduler = sched.scheduler(time.time, time.sleep)

    sequence_base = 0
    received_sequence_numbers = set()

    def sender_master_listener():
        global sender_master_address
        global number_of_segments
        global sender_master_syn_ack_timer
        print("Listening to sender master")
        while True:
            packet, address = inter_socket.recvfrom(PacketSize.SENDER_MASTER_TO_RECEIVER)
            decoded_packet = pickle.loads(packet)
            packet_type = decoded_packet['packet_type']
            print("Packet received from sender master, packet type {}".format(packet_type))
            if packet_type == PacketType.SYN:
                print("Connected to sender master")
                sender_master_address = address
                number_of_segments = decoded_packet['number_of_segments']
                syn_ack_packet = { 'packet_type': PacketType.SYN_ACK, 'slave_addresses': list(slave_addresses.values()) }
                inter_socket.sendto(pickle.dumps(syn_ack_packet), address)
                sender_master_syn_ack_timer = threading.Timer(SENDER_SYN_ACK_TIMEOUT, sender_syn_ack_timeout_handler, [syn_ack_packet, SENDER_SYN_ACK_ATTEMPTS])
                sender_master_syn_ack_timer.start()
            elif packet_type == PacketType.SYN_ACK_RECEIVED:
                sender_master_syn_ack_timer.cancel()
            elif packet_type == PacketType.ACK_RECEIVED:
                sender_master_ack_timer.cancel()
            elif packet_type == PacketType.RECEIVER_ADDRESSES_ACK:
                sender_master_receiver_addresses_timer.cancel()
            elif packet_type == PacketType.FIN_ACK:
                print("Sender master terminated, start terminating all receiver slaves")
                sender_master_fin_timer.cancel()
                fin_ack_received_packet = { 'packet_type': PacketType.FIN_ACK_RECEIVED }
                inter_socket.sendto(pickle.dumps(fin_ack_received_packet), address)
                fin_packet = { 'packet_type': PacketType.FIN }
                for slave_id, slave_address in slave_addresses:
                    intra_socket.sendto(pickle.dumps(fin_packet), slave_address)
                    slave_fin_timers[slave_id] = threading.Timer(SLAVE_FIN_TIMEOUT, slave_fin_timeout_handler, [slave_id, fin_packet, slave_address, SLAVE_FIN_ATTEMPTS])
                    slave_fin_timers[slave_id].start()

    def receiver_slave_listener():
        global sender_master_receiver_addresses_timer
        print("Listening to receiver slaves")
        while True:
            packet, address = intra_socket.recvfrom(PacketSize.RECEIVER_SLAVE_TO_MASTER)
            decoded_packet = pickle.loads(packet)
            slave_id = decoded_packet['slave_id']
            packet_type = decoded_packet['packet_type']
            print("Packet received from receiver slave {}, packet type {}".format(slave_id, packet_type))
            if packet_type == PacketType.SYN:
                slave_addresses[slave_id] = address
                syn_ack_packet = { 'packet_type': PacketType.SYN_ACK, 'output_file': output_file }
                intra_socket.sendto(pickle.dumps(syn_ack_packet), address)
                print("Connected to receiver slave {}".format(slave_id))
                # notice sender master about all available receiver slaves
                if sender_master_address is not None:
                    print("Noticing sender master about all new slaves {}".format(list(slave_addresses.values())))
                    receiver_addresses_packet = { 'packet_type': PacketType.RECEIVER_ADDRESSES, 'slave_addresses': list(slave_addresses.values()) }
                    inter_socket.sendto(pickle.dumps(receiver_addresses_packet), sender_master_address)
                    sender_master_receiver_addresses_timer = threading.Timer(RECEIVER_ADDRESSES_TIMEOUT, receiver_addresses_timeout_handler, [receiver_addresses_packet, RECEIVER_ADDRESSES_ATTEMPTS])
                    sender_master_receiver_addresses_timer.start()
                if not slave_id in slave_syn_ack_timers:
                    slave_syn_ack_timers[slave_id] = threading.Timer(SLAVE_SYN_ACK_TIMEOUT, slave_syn_ack_timeout_handler, [slave_id, syn_ack_packet, address, SLAVE_SYN_ACK_ATTEMPTS])
                    slave_syn_ack_timers[slave_id].start()
            elif packet_type == PacketType.SYN_ACK_RECEIVED:
                slave_syn_ack_timers[slave_id].cancel()
            elif packet_type == PacketType.PACKET_RECEIVED:
                sequence_number = decoded_packet['sequence_number']
                print("Packet with sequence {} received from slave {}".format(sequence_number, slave_id))
                received_sequence_numbers.add(sequence_number)
                packet_received_ack_packet = { 'packet_type': PacketType.PACKET_RECEIVED_ACK }
                intra_socket.sendto(pickle.dumps(packet_received_ack_packet), address)
            elif packet_type == PacketType.FIN_ACK:
                print("Slave {} terminated".format(slave_id))
                slave_fin_timers[slave_id].cancel()
                fin_ack_received_packet = { 'packet_type': PacketType.FIN_ACK_RECEIVED }
                intra_socket.sendto(pickle.dumps(fin_ack_received_packet), address)
                if not slave_id in slave_addresses:
                    # finished termination with this slave, removing it from the list
                    slave_addresses.pop(slave_id)
                    if len(slave_addresses) == 0:
                        print("All slaves are terminated, closing")
                        # all slaves and sender master is terminated, terminate receiver master here
                        terminate()

    def receiver_addresses_timeout_handler(receiver_addresses_packet, remaining_attempts):
        global sender_master_receiver_addresses_timer
        if remaining_attempts > 0:
            print("RECEIVER_ADDRESSES timeout, retry left {}".format(remaining_attempts))
            inter_socket.sendto(pickle.dumps(receiver_addresses_packet), sender_master_address)
            sender_master_receiver_addresses_timer = threading.Timer(RECEIVER_ADDRESSES_TIMEOUT, receiver_addresses_timeout_handler, [receiver_addresses_packet, remaining_attempts - 1])
            sender_master_receiver_addresses_timer.start()

    def sender_syn_ack_timeout_handler(syn_ack_packet, remaining_attempts):
        global sender_master_syn_ack_timer
        if remaining_attempts > 0:
            print("sender SYN_ACK timeout, retry left {}".format(remaining_attempts))
            inter_socket.sendto(pickle.dumps(syn_ack_packet), sender_master_address)
            sender_master_syn_ack_timer = threading.Timer(SENDER_SYN_ACK_TIMEOUT, sender_syn_ack_timeout_handler, [syn_ack_packet, remaining_attempts - 1])
            sender_master_syn_ack_timer.start()

    def slave_syn_ack_timeout_handler(slave_id, syn_ack_packet, slave_address, remaining_attempts):
        # recursively resend the syn_ack packet until no remaining attempts left we we assume that slave is down
        if remaining_attempts > 0:
            print("slave SYN_ACK timeout on slave {}, retry left {}".format(slave_id, remaining_attempts))
            intra_socket.sendto(pickle.dumps(syn_ack_packet), slave_address)
            slave_syn_ack_timers[slave_id] = threading.Timer(SLAVE_SYN_ACK_TIMEOUT, slave_syn_ack_timeout_handler, [slave_id, syn_ack_packet, slave_address, remaining_attempts - 1])
            slave_syn_ack_timers[slave_id].start()

    def slave_fin_timeout_handler(slave_id, fin_packet, slave_address, remaining_attempts):
        # recursively resend the fin packet until no remaining attempts left we we assume that slave is down
        if remaining_attempts > 0:
            print("slave FIN timeout on slave {}, retry left {}".format(slave_id, remaining_attempts))
            intra_socket.sendto(pickle.dumps(fin_packet), slave_address)
            slave_fin_timers[slave_id] = threading.Timer(SLAVE_FIN_TIMEOUT, slave_fin_timeout_handler, [slave_id, fin_packet, slave_address, remaining_attempts - 1])
            slave_fin_timers[slave_id].start()

    def sender_fin_timeout_handler(fin_packet, remaining_attempts):
        global sender_master_fin_timer
        if remaining_attempts > 0:
            print("sender FIN timeout, retry left {}".format(remaining_attempts))
            inter_socket.sendto(pickle.dumps(fin_packet), sender_master_address)
            sender_master_fin_timer = threading.Timer(SENDER_FIN_TIMEOUT, sender_fin_timeout_handler, [fin_packet, remaining_attempts - 1])
            sender_master_fin_timer.start()
    
    def ack_timeout_handler(ack_packet, remaining_attempts):
        global sender_master_ack_timer
        if remaining_attempts > 0:
            print("ACK timeout, retry left {}".format(remaining_attempts))
            inter_socket.sendto(pickle.dumps(ack_packet), sender_master_address)
            sender_master_ack_timer = threading.Timer(ACK_TIMEOUT, ack_timeout_handler, [ack_packet, remaining_attempts - 1])
            sender_master_ack_timer.start()
    
    def ack_schedule_event():
        global sender_master_ack_timer
        global sender_master_fin_timer
        print("Scheduled ack event")
        missing_sequence_numbers = []
        min_unack_sequence_number = number_of_segments
        if number_of_segments is not None and sender_master_address is not None and sequence_base < number_of_segments:
            for i in range(base, number_of_segments):
                if not received_sequence_numbers.contains(i):
                    missing_sequence_numbers.append(i)
                    min_unack_sequence_number = min(min_unack_sequence_number, i)
            sequence_base = min_unack_sequence_number
            print("Current sequence base {}".format(sequence_base))
            print("Current missing sequence numbers {}".format(missing_sequence_numbers))
            ack_packet = { 'packet_type': PacketType.ACK, 'missing_sequence_numbers': missing_sequence_numbers }
            inter_socket.sendto(pickle.dumps(ack_packet), sender_master_address)
            print("Cumulated ACK packet sent")
            if sender_master_ack_timer is not None:
                # terminate previously unsent ack packet, sending the updated one
                sender_master_ack_timer.cancel()
            sender_master_ack_timer = threading.Timer(ACK_TIMEOUT, ack_timeout_handler, [ack_packet, ACK_ATTEMPTS])
            sender_master_ack_timer.start()
            if sequence_base == number_of_segments:
                print("All packets received, start termination")
                fin_packet = { 'packet_type': PacketType.FIN }
                inter_socket.sendto(pickle.dumps(fin_packet), sender_master_address)
                sender_master_fin_timer = threading.Timer(SENDER_FIN_TIMEOUT, sender_fin_timeout_handler, [fin_packet, SENDER_FIN_ATTEMPTS])
                sender_master_fin_timer.start()

    def terminate():
        # all slaves and receiver master is terminated, terminate sender master here
        inter_socket.close()
        intra_socket.close()
        os._exit(1)

    slaves_listener_thread = threading.Thread(target=receiver_slave_listener)
    sender_master_listener_thread = threading.Thread(target=sender_master_listener)
    slaves_listener_thread.start()
    sender_master_listener_thread.start()
    ack_scheduler.enter(ACK_SCHEDULER_DELAY, 1, ack_schedule_event)

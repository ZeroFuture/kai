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
PING_TIMEOUT = 1

SENDER_SYN_ACK_ATTEMPTS = 3
SLAVE_SYN_ACK_ATTEMPTS = 3
SLAVE_FIN_ATTEMPTS = 3
SENDER_FIN_ATTEMPTS = 3
RECEIVER_ADDRESSES_ATTEMPTS = 3
ACK_ATTEMPTS = 3
PING_ATTEMPTS = 3

ACK_SCHEDULER_DELAY = 3
PING_SCHEDULER_DELAY = 5
MAX_STALE_ATTEMPTS = 3

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
    slave_inter_ports = {}
    sender_master_address = None
    number_of_segments = None

    slave_syn_ack_timers = {}
    slave_fin_timers = {}
    slave_ping_timers = {}
    sender_master_syn_ack_timer = None
    sender_master_receiver_addresses_timer = None
    sender_master_ack_timer = None
    sender_master_fin_timer = None
    ack_scheduler = sched.scheduler(time.time, time.sleep)
    ping_scheduler = sched.scheduler(time.time, time.sleep)

    sequence_base = 0
    sequence_ceil = 0
    stale_counter = 0
    slave_received_sequences = {}
    received_sequence_set = set()

    def sender_master_listener():
        global sender_master_address
        global number_of_segments
        global sender_master_syn_ack_timer
        print("Listening to sender master")
        while True:
            packet, address = inter_socket.recvfrom(PacketSize.SENDER_MASTER_TO_RECEIVER)
            decoded_packet = pickle.loads(packet)
            packet_type = decoded_packet['packet_type']
            print("Packet received from sender master, packet type {}".format(PacketType.translate(packet_type)))
            if packet_type == PacketType.SYN:
                print("Connected to sender master")
                sender_master_address = address
                number_of_segments = decoded_packet['number_of_segments']
                syn_ack_packet = { 'packet_type': PacketType.SYN_ACK, 'slave_addresses': list(slave_inter_ports.values()) }
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
                for slave_id, slave_address in slave_addresses.items():
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
            print("Packet received from receiver slave {}, packet type {}".format(slave_id, PacketType.translate(packet_type)))
            if packet_type == PacketType.SYN:
                slave_addresses[slave_id] = address
                inter_port = decoded_packet['inter_port']
                slave_inter_ports[slave_id] = inter_port
                syn_ack_packet = { 'packet_type': PacketType.SYN_ACK, 'output_file': output_file }
                intra_socket.sendto(pickle.dumps(syn_ack_packet), address)
                print("Connected to receiver slave {}".format(slave_id))
                # notice sender master about all available receiver slaves
                if sender_master_address != None:
                    inter_ports = list(slave_inter_ports.values())
                    print("Noticing sender master about current available slaves {}".format(inter_ports))
                    receiver_addresses_packet = { 'packet_type': PacketType.RECEIVER_ADDRESSES, 'slave_addresses': inter_ports }
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
                received_sequences = decoded_packet['received_sequences']
                print("Packet with sequence {} received from slave {}".format(sequence_number, slave_id))
                received_sequence_set.add(sequence_number)
                slave_received_sequences[slave_id] = received_sequences
                packet_received_ack_packet = { 'packet_type': PacketType.PACKET_RECEIVED_ACK }
                intra_socket.sendto(pickle.dumps(packet_received_ack_packet), address)
            elif packet_type == PacketType.PING_ACK and slave_id in slave_ping_timers:
                slave_ping_timers[slave_id].cancel()
            elif packet_type == PacketType.FIN_ACK:
                print("Slave {} terminated".format(slave_id))
                slave_fin_timers[slave_id].cancel()
                fin_ack_received_packet = { 'packet_type': PacketType.FIN_ACK_RECEIVED }
                intra_socket.sendto(pickle.dumps(fin_ack_received_packet), address)
                if not slave_id in slave_addresses:
                    # finished termination with this slave, removing it from the list
                    slave_addresses.pop(slave_id)
                    if len(slave_addresses) == 0:
                        print("All slaves are terminated, generate output file")
                        generate_output_file()
                        print("Output file generated, closing")
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
    
    def ping_timeout_handler(slave_id, ping_packet, slave_address, remaining_attempts):
        global sequence_base
        if remaining_attempts > 0:
            print("PING timeout, retry left {}".format(remaining_attempts))
            intra_socket.sendto(pickle.dumps(ping_packet), slave_address)
            slave_ping_timers[slave_id] = threading.Timer(PING_TIMEOUT, ping_timeout_handler, [slave_id, ping_packet, slave_address, remaining_attempts - 1])
            slave_ping_timers[slave_id].start()
        else:
            # slave down, removing from the list
            print("Slave {} down, health check reached max attempts".format(slave_id))
            slave_addresses.pop(slave_id)
            slave_ping_timers.pop(slave_id)
            slave_inter_ports.pop(slave_id)
            inter_ports = list(slave_inter_ports.values())
            print("Noticing sender master about current available slaves {}".format(inter_ports))
            receiver_addresses_packet = { 'packet_type': PacketType.RECEIVER_ADDRESSES, 'slave_addresses': inter_ports }
            inter_socket.sendto(pickle.dumps(receiver_addresses_packet), sender_master_address)
            sender_master_receiver_addresses_timer = threading.Timer(RECEIVER_ADDRESSES_TIMEOUT, receiver_addresses_timeout_handler, [receiver_addresses_packet, RECEIVER_ADDRESSES_ATTEMPTS])
            sender_master_receiver_addresses_timer.start()
    
    def ack_schedule_event():
        global sender_master_ack_timer
        global sender_master_fin_timer
        global sequence_base
        global sequence_ceil
        global stale_counter
        print("Scheduled ack event")
        missing_sequence_numbers = []
        min_unack_sequence_number = number_of_segments
        if number_of_segments != None and sender_master_address != None and sequence_base < number_of_segments:
            max_ack_sequence_number = sequence_ceil
            for i in range(sequence_ceil, number_of_segments):
                if i in received_sequence_set:
                    max_ack_sequence_number = i
            if sequence_ceil == max_ack_sequence_number:
                # sequence_ceil has not been moved since last scheduled event
                stale_counter += 1
                if stale_counter >= MAX_STALE_ATTEMPTS:
                    # at this point we think all packets from sequence_ceil to number_of_segments has been lost, adding them to missing sequence numbers
                    for i in range(sequence_ceil + 1, number_of_segments):
                        missing_sequence_numbers.append(i)
            else:
                # reset stale_counter
                stale_counter = 0
            sequence_ceil = max_ack_sequence_number
            for i in range(sequence_ceil - 1, sequence_base - 1, -1):
                if not i in received_sequence_set:
                    missing_sequence_numbers.append(i)
                    min_unack_sequence_number = i
            sequence_base = min_unack_sequence_number
            print("Current sequence base {}".format(sequence_base))
            print("Current sequence ceil {}".format(sequence_ceil))
            print("Current missing sequence numbers {}".format(missing_sequence_numbers))
            ack_packet = { 'packet_type': PacketType.ACK, 'missing_sequence_numbers': missing_sequence_numbers }
            inter_socket.sendto(pickle.dumps(ack_packet), sender_master_address)
            print("Cumulated ACK packet sent")
            if sender_master_ack_timer != None:
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
            else:
                # set up for next scheduler
                ack_scheduler.enter(ACK_SCHEDULER_DELAY, 1, ack_schedule_event)
                ack_scheduler.run()
        else:
            # set up for next scheduler
            ack_scheduler.enter(ACK_SCHEDULER_DELAY, 1, ack_schedule_event)
            ack_scheduler.run()

    def ping_schedule_event():
        for slave_id, slave_address in slave_addresses.items():
            ping_packet = { 'packet_type': PacketType.PING }
            intra_socket.sendto(pickle.dumps(ping_packet), slave_address)
            slave_ping_timers[slave_id] = threading.Timer(PING_TIMEOUT, ping_timeout_handler, [slave_id, ping_packet, slave_address, PING_ATTEMPTS])
            slave_ping_timers[slave_id].start()
        ping_scheduler.enter(PING_SCHEDULER_DELAY, 1, ping_schedule_event)
        ping_scheduler.run()

    def generate_output_file():
        packet_positions = [None] * number_of_segments
        for slave_id, received_sequences in slave_received_sequences.items():
            for i in range(0, len(received_sequences)):
                sequence = received_sequences[i]
                packet_positions[sequence] = (slave_id, i)
        with open(output_file, 'w') as of:
            for slave_id, index in packet_positions:
                slave_file = output_file.split('.')[0] + '_' + slave_id + '.' + output_file.split('.')[1]
                data = None
                with open(slave_file) as sf:
                    sf.seek(index * PacketSize.DATA_SEGMENT)
                    data = sf.read(PacketSize.DATA_SEGMENT)
                of.write(data)

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
    ack_scheduler.run()
    ping_scheduler.enter(PING_SCHEDULER_DELAY, 1, ping_schedule_event)
    ping_scheduler.run()

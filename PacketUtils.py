class PacketSize:
    DATA_SEGMENT = 1024
    SENDER_MASTER_TO_SLAVE = 1024
    SENDER_SLAVE_TO_MASTER = 1024
    RECEIVER_MASTER_TO_SLAVE = 1024
    RECEIVER_SLAVE_TO_MASTER = 4096
    SENDER_MASTER_TO_RECEIVER = 2048
    RECEIVER_MASTER_TO_SENDER = 2048
    SENDER_SLAVE_TO_RECEIVER = 2048

class PacketType:
    SYN = 0
    SYN_ACK = 1
    SYN_ACK_RECEIVED = 2
    FIN = 3
    FIN_ACK = 4
    FIN_ACK_RECEIVED = 5
    ASSIGN = 6
    RECEIVER_ADDRESSES = 7
    RECEIVER_ADDRESSES_ACK = 8
    DATA = 9
    ACK = 10
    ACK_RECEIVED = 11
    PING = 12
    PING_ACK = 13

    ALL_PACKET_TYPES = ['SYN', 'SYN_ACK', 'SYN_ACK_RECEIVED', 'FIN', 'FIN_ACK', 
    'FIN_ACK_RECEIVED', 'ASSIGN', 'RECEIVER_ADDRESSES', 'RECEIVER_ADDRESSES_ACK', 
    'DATA', 'ACK', 'ACK_RECEIVED', 'PING', 'PING_ACK']

    @staticmethod
    def translate(packet_type):
        return PacketType.ALL_PACKET_TYPES[packet_type]

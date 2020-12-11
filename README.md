# Kai

Developed by Zhidong Qu, Jiayan Wei, and Ziqi Tang.

## Running Kai on MacOS through automated script 

There are two simple examples for the protocol. `single_slave_example.py` runs a single sender slave and a single receiver slave for the data transmission. `multiple_slave_example.py` runs 3 sender slave and 3 receiver slaves by default which is also configurable by changing the `number_of_sender_slaves` and `number_of_receiver_slaves` variables in the script. Both examples assumes that you have the default ports open in your machine, if you run into exceptions related to ports, try to manually modify the port numbers in the script to whatever you have available on your machine. 

Currently the script can only run on Mac OS, if you are running this on Mac, you also needs to install a packet `applescript` by running `pip3 install applescript` or `pip install applescript`. Finally, you can run both scripts with `python3 <script name>` under the working directory.

If you can not run the script above or want to manually run the whole protocol, see the tutorial below.


## Running Kai manually

To start the whole protocol, you should start the `ReceiverMaster.py` first, then start either `SenderMaster.py` or all the receiver slaves `ReceiverSlave.py`. Finally, you need to start all the sender slaves `SenderSlave.py`. 


### Starting `ReceiverMaster.py`

To start `ReceiverMaster.py`, run `python3 ReceiverMaster.py <inter_port> <intra_port> <output_file>`

`inter_port` is the port used to communicate with the sender master

`intra_port` is the port used to communicate with the receiver slaves

### Starting `SenderMaster.py`

To start `SenderMaster.py`, run `python3 SenderMaster.py <inter_port> <intra_port> <receiver_master_ip> <receiver_master_port> <input_file>`

`inter_port` is the port used to communicate with the receiver master

`intra_port` is the port used to communicate with the sender slaves

`receiver_master_port` is the same as the `inter_port` of the receiver master

### Starting `ReceiverSlave.py`

To start `ReceiverSlave.py`, run `python3 ReceiverSlave.py <intra_port> <inter_port> <receiver_master_DNS_name> <receiver_master_port>`

`intra_port` is the port used to communicate with the receiver master

`inter_port` is the port used to communicate with the sender slaves

`receiver_master_port` is the same as the `intra_port` of the receiver master

### Starting `SenderSlave.py`

To start `SenderSlave.py`, run `python3 SenderSlave.py <intra_port> <inter_port> <sender_master_DNS_name> <sender_master_port>`

`intra_port` is the port used to communicate with the sender master

`inter_port` is the port used to communicate with the receiver slaves

`sender_master_port` is the same as the `intra_port` of the sender master

## Configurations

All configurations are available on the top of each file, including `PacketUtils.py` which contains all the data segment and packet sizes. 
The default setting is to have 3 second timeout on most of the messages that requires a acknowledgement, and there is a 3 time retry as well. PING will be sent every second and there is a 5 times retry before the master node to consider a slave being down. 
By default the packets will not be dropped but it is editable by changing the `RANDOM_DROP_PROB` variable in each file. 
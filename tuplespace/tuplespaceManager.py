#!/usr/bin/env python3

import json
import socket
import struct
import sys

import proxy
import config

def notif_to_dict(notification):
    """converts a notification decoded from the network into a dictionary"""
    notification = notification.replace(" ", ",", 2) # comma separate our three fields
    notification = notification.split(",", 2)

    return { "name": notification[0],
             "event": notification[1],
             "message": notification[2]
    }

def replay_history(address):
    """Replays microblog history to the adapter referenced by address"""
    ts = proxy.TupleSpaceAdapter(address)

    with open(f'.replicationLog-{ts_name}', mode='r') as m:
        # connect to newly joined adapter
        ts = proxy.TupleSpaceAdapter(address)
        # read file in as list of strings
        lines = m.read().splitlines()
        # read each line as json
        lines = [json.loads(l) for l in lines]
        # filter out nameserver events
        lines = [l for l in lines if l['name'] != "nameserv"]
        # filter for all the writes and takes
        lines = [l for l in filter(lambda li: "write"
                                    in li['event'] or "take" in
                                    li['event'], lines)]
        for line in lines:
            print(f'recovery: {line}')
            if line['event'] == 'write':
                print(f'replaying {line["message"]} to {ts}')
                # NOTE: eval is very fragile, is there a better
                # way to do this?
                ts._out(eval(line['message']))
            elif line['event'] == 'take':
                print(f'replaying {line["message"]} to {ts}')
                _ = ts._inp(eval(line['message']))  # we don't care about the return value
            else:
                print("Something went wrong!")
                return

# per <https://en.wikipedia.org/wiki/User_Datagram_Protocol>
MAX_UDP_PAYLOAD = 65507
config = config.read_config()

ts_name      = config['name']
adapter_host = config['adapter']['host']
adapter_port = config['adapter']['port']

adapter_uri = f'http://{adapter_host}:{adapter_port}'
ts = proxy.TupleSpaceAdapter(adapter_uri)

print(f'Connected to tuplespace {ts_name} on {adapter_uri}')

# code.interact(local=locals())

def main(address, port):
    # See <https://pymotw.com/3/socket/multicast.html> for details
    server_address = ('', int(port))

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(server_address)

    group = socket.inet_aton(address)
    mreq = struct.pack('4sL', group, socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    print(f"Listening on udp://{address}:{port}")
    with open(f'.replicationLog-{ts_name}', "w+") as log_file:
        try:
            while True:
                data, _ = sock.recvfrom(MAX_UDP_PAYLOAD)
                notification = data.decode()
                notif_dict = notif_to_dict(notification)

                print(notif_dict)
                # log json to file
                log_file.write(json.dumps(notif_dict))
                log_file.write('\n')
                log_file.flush()

                # Problem: How can we prevent the events we receive
                # from this recovery from being added to the log?

                if notif_dict['event'] == 'adapter':
                    # TODO: either 'adapter' or 'start' was received and replication needs to be performed
                    # 1. Attach to tuplespace of newly joined user. (i.e. extract address from notification)
                    replay_history(notif_dict['message'])
                elif notif_dict['event'] == 'write':
                    listToWrite = tuple(eval(notif_dict['message']))
                    ts._out(listToWrite)
                elif notif_dict['event'] == 'take':
                    listToTake = tuple(eval(notif_dict['message']))
                    ts._in(listToTake)
                else:
                    pass
        except Exception as e:
            print(e)
            sock.close()


def usage(program):
    print(f'Usage: {program} ADDRESS PORT', file=sys.stderr)
    sys.exit(1)


if __name__ == '__main__':
    print(*sys.argv[1:3])
    sys.exit(main(sys.argv[1], sys.argv[2]))

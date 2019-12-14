#!/usr/bin/env python3

import json
import sys
import struct
import socket

import proxy

# Recovery:
#
# The recovery feature should listen for an incoming
# *adapter* event on the multicast group, attach to
# the adapter located at *address*, and then replay
# all *takes* and *writes* in the order they were
# logged to the joining adapter/tuplespace pair.

# Potential Issues:
#
# Right now we are only looking for the *adapter* event, and are
# assuming that each adapter event is associated with a paired
# tuplespace *start* event. Obviously, if the associated tuplespace
# for the adapter is not actually online, all tuplespace operations
# will fail when trying to replay to the server. Additional logic is
# needed here to handle all the variable edge cases.

# Because the recovery server is running on a single thread of
# control, we will be unable to respond to (i.e log) any multicast
# events while we are recovering a tuplespace. Potential issues are to
# create a queue for incoming requests to await processing, or to spin
# off a separate thread for each tuplespace that requires recovery.

# Another issue that this design contains is that recovery only works
# a single time. Without any additional logic, the tuplespace that is
# recovered will also multicast its write and take events, which will
# consequently be logged in the manifest file. Potential avenues of
# approach for this are to introduce a lock, where the manifest file
# becomes a lockable resource, to prevent us reading and writing
# simultaneuously.

# per <https://en.wikipedia.org/wiki/User_Datagram_Protocol>
MAX_UDP_PAYLOAD = 65507


def main(address, port):

    def replay_history(address):
        """Replays microblog history to the adapter referenced by address"""
        ts = proxy.TupleSpaceAdapter(address)

        with open(".manifest", mode='r') as m:
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

            # PROBLEM: Because we are not currently able to filter out
            # repeated events from operations replicated to other
            # tuplespaces, any node must be recovered, will receive N
            # copies of all tuples written to the log so far, where N
            # is the number of nodes online before the node was
            # recovered

            # Potential solution: If we had a way to imbue each
            # message being written with a unique identifier, such
            # that each copy of the message in all tuplespaces share
            # the identifier, we can allow the first one to be the
            # source of truth, allowing us to keep a running set of
            # uuid we have seen so far, and not allow messages with
            # uuid's we have already seen to play. That is, we want to
            # ensure that each discrete tuplespace operation is played
            # exactly once.

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


    def notif_to_dict(notification):
        """converts a notification decoded from the network into a dictionary"""
        notification = notification.replace(" ", ",", 2) # comma separate our three fields
        notification = notification.split(",", 2)

        return { "name": notification[0],
                 "event": notification[1],
                 "message": notification[2]
        }

    ####################
    # BEGIN MAIN
    ####################

    server_address = ('', int(port))

    # create a UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # bind the socket to the server's address
    sock.bind(server_address)

    # define the multicast group
    group = socket.inet_aton(address)
    mreq = struct.pack('4sL', group, socket.INADDR_ANY)

    # subscribe to the multicast group
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    print(f"Listening on udp://{address}:{port}")

    with open(".manifest", mode='w+') as log_file:
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
                if notif_dict['event'] == "adapter":
                    replay_history(notif_dict['message']) # recover the adapter's tuplespace
        except Exception as e:
            print(e)
            sock.close()


def usage(program):
    print(f'Usage: {program} ADDRESS PORT', file=sys.stderr)
    sys.exit(1)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        usage(sys.argv[0])

    sys.exit(main(*sys.argv[1:]))

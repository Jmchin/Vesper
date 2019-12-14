#!/usr/bin/env python3

# nameserver.py

# This server program listens for tuplespace and adapter start events,
# and upon receipt persists the event to its own local tuplespace via
# XML-RPC.

# 1. nameserver binds a socket to listen for IP multicast traffic for
# the microblogging group.

# 2. inspects incoming notifcations, and logs all tuplespace and
# adapter start events

# 3. when receiving request from client for a username lookup, will
# return the address bound to the username, or throw a user not found
# exception


import sys
import struct
import socket
import proxy

MAX_UDP_PAYLOAD = 65507

def main(address, port):

    def notif_to_dict(notification):
        """Converts a notification decoded from the network into a dictionary"""
        notification = notification.replace(" ", ",", 2) # comma separate our three fields
        notification = notification.split(",", 2)

        return { "name": notification[0],
                 "event": notification[1],
                 "message": notification[2]
        }


    server_address = ('', int(port))

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    sock.bind(server_address)

    group = socket.inet_aton(address)
    mreq = struct.pack('4sL', group, socket.INADDR_ANY)

    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    print(f'Listening on udp://{address}:{port}')

    try:

        # TODO: Listen for incoming connections from the mblog client

        # The nameserver needs to accept an incoming connection from
        # the client, where the message requests an address binding
        # for a username in the microblog group.

        # Once the request is received, the nameserver should look up
        # the binding for the name, and if it exists, return it to the
        # client. A list of all other bindings should also be returned
        # to the client so that the tuplespace operation is played to
        # everyone.

        # connect to our tuplespace
        ts = proxy.TupleSpaceAdapter("http://localhost:8001")

        while True:
            data, _ = sock.recvfrom(MAX_UDP_PAYLOAD)
            notification = data.decode()
            notif_dict = notif_to_dict(notification)


            if notif_dict["event"] == "start":
                print(f'nameserver: {notif_dict}')
                # first, remove the existing binding
                ts._inp((notif_dict["name"], "start", str))
                # second, add the new binding
                ts._out((notif_dict["name"], "start", notif_dict["message"]))

            if notif_dict["event"] == "adapter":
                print(f'nameserver: {notif_dict}')
                # get the users tuples
                users = ts._inp(("users", None))
                # create the users dict if it does not exist
                users = users if users else ("users", dict())
                # build the binding
                bindings = users[1]
                # set key-value pair, {name : message}
                bindings[notif_dict["name"]] = notif_dict["message"]
                # write the new bindings list to the tuplespace
                ts._out(("users", bindings))

                #TODO: probably need to remove this, these bindings
                # not needed
                ts._inp((notif_dict["name"], notif_dict["event"], str))
                ts._out((notif_dict["name"], notif_dict["event"], notif_dict["message"]))

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

#!/usr/bin/env python3

import sys
import socket
import proxy
import config


# the third argument must be surrounded by quotations when invoked
# from the command line (e.g ./mblog.py alice distsys "hello, world!")
def main(tsName, topic, text):
    if len(sys.argv) < 3:
        return
    myTuple = (tsName, topic, text) = sys.argv[1:]
    # print(myTuple)

    # TODO: Don't connect directly to the nameserver's tuplespace
    # adapter, instead we want to open up a connection to the
    # nameserver middleware layer, which will handle all of our
    # requests

    # create connection to nameserver
    ts = proxy.TupleSpaceAdapter('http://localhost:8001')

    # quickly check if the nameserver has seen tsName yet
    if ts._rdp((tsName, "adapter", str)) is not None:
        users_list = ts._rdp(("users", None))
        bindings = users_list[1]

        # write the message to all the users
        for user in [k for k in bindings.keys()]:
            try:
                tsa = proxy.TupleSpaceAdapter(bindings[user])

                # if tuplespace/adapter pair is not up, the client
                # will raise an exception because we are trying to
                # connect to a dead node
                tsa._out(myTuple)
            except Exception as e:
                print(e)
    else:
        print(f'{tsName} does not exist.')


# TODO: Clean this up!
if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("bad argument")
    sys.exit(main(*sys.argv[1:]))

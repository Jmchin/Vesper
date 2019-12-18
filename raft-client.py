#!/usr/bin/env python3

# a command line client for interacting with our tuplespace microblog
# platform powered on the raft consensus algorithm


import argparse
import sys
import zmq

# if __name__ == "__main__":


#     # just for testing
#     sock = zmq.Context().socket(zmq.REQ)

#     message = {
#         "type": "Get",
#         "payload": {
#             "user": "",
#             "topic": "",
#             "content": ""}
#     }

#     # connect to our leader
#     sock.connect(f"tcp://127.0.0.1:{port}")

def redirectToLeader(server_address, message):
    # initialize a zmq context
    context = zmq.Context()
    # setup REQ socket
    sock = context.socket(zmq.REQ)
    # connect the socket to the server
    sock.connect(server_address)

    resp = None
    while not resp:
        try:
            sock.send_json(message)
            resp = sock.recv_json()
        except Exception as e:
            print(e)
    return resp

    # type = message["type"]
    # # looping until someone tells he is the leader
    # while True:
    #     # switching between "get" and "put"
    #     if type == "get":
    #         try:
    #             response = sock.send_json(message, flags=zmq.NOBLOCK)
    #         except Exception as e:
    #             return e
    #     else:
    #         try:
    #             response = sock.send_json(message, flags=zmq.NOBLOCK)
    #         except Exception as e:
    #             return e

    #     # if valid response and an address in the "message" section in reply
    #     # redirect server_address to the potential leader
    #     if "leaderId" in response:
    #         leader_id = response["leaderId"]
    #         if "message" in payload:
    #             server_address = payload["message"] + "/request"
    #         else:
    #             break
    #     else:
    #         break
    # # if type == "get":
    # return response.json()
    # else:
    #     return response


def put(addr, user, topic, content):
    server_address = addr
    payload = {'user': user, 'topic': topic, 'content': content}
    message = {"type": "Put", "payload": payload}
    # redirecting till we find the leader, in case of request during election
    print(redirectToLeader(server_address, message))

# Proxy is expecting python types when mapping templates out for
# Py=>Rb conversion


def get(addr, tupl):
    server_address = addr
    payload = {"pattern": tupl}
    message = {"type": "Get", "payload": payload}
    print(redirectToLeader(server_address, message))


if __name__ == "__main__":
    # python client.py 127.0.0.1:5000 get -user alice -topic distsys
    # => [("alice", "distsys", "foo"), ("alice", "distsys", "bar")...]
    parser = argparse.ArgumentParser()
    parser.add_argument("hostname", nargs='?',
                        default="tcp://127.0.0.1:50000",
                        help="{protocol}://{address}:{port}")       # host to connect
    parser.add_argument("operation", choices=[
                        'put', 'get'], help="One of 'get' or 'put'")
    parser.add_argument("-u", "--user")     # user
    parser.add_argument("-t", "--topic")    # topic/channel
    parser.add_argument("-m", "--message")
    args = parser.parse_args()

    if args.operation == "get":
        user, topic, msg = args.user, args.topic, args.message
        pattern = (user, topic, msg)
        get(args.hostname, pattern)
    elif args.operation == "put":
        user, topic, msg = args.user, args.topic, args.message
        if user and topic and msg:
            put(args.hostname, user, topic, msg)
        else:
            print(f'Error: PUT must have "user", "topic" and "msg"')

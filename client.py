import argparse
import sys, requests


def redirectToLeader(server_address, message):
    type = message["type"]
    # looping until someone tells he is the leader
    while True:
        # switching between "get" and "put"
        if type == "get":
            try:
                response = requests.get(server_address,
                                        json=message,
                                        timeout=1)
            except Exception as e:
                return e
        else:
            try:
                response = requests.put(server_address,
                                        json=message,
                                        timeout=1)
            except Exception as e:
                return e

        # if valid response and an address in the "message" section in reply
        # redirect server_address to the potential leader
        if response.status_code == 200 and "payload" in response.json():
            payload = response.json()["payload"]
            if "message" in payload:
                server_address = payload["message"] + "/request"
            else:
                break
        else:
            break
    # if type == "get":
    return response.json()
    # else:
    #     return response

def put(addr, user, topic, content):
    server_address = addr + "/request"
    payload = {'user': user, 'topic': topic, 'content': content}
    message = {"type": "put", "payload": payload}
    # redirecting till we find the leader, in case of request during election
    print(redirectToLeader(server_address, message))

# Proxy is expecting python types when mapping templates out for
# Py=>Rb conversion
def get(addr, tupl):
    server_address = addr + "/request"
    payload = {"pattern": tupl}
    message = {"type": "get", "payload": payload}
    print(redirectToLeader(server_address, message))


if __name__ == "__main__":
    # python client.py 127.0.0.1:5000 get -user alice -topic distsys
    # => [("alice", "distsys", "foo"), ("alice", "distsys", "bar")...]
    parser = argparse.ArgumentParser()
    parser.add_argument("hostname", nargs='?',
                        default="http://127.0.0.1:5000",
                        help="{protocol}://{address}:{port}")       # host to connect
    parser.add_argument("operation", choices=['put','get'], help="One of 'get' or 'put'")
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

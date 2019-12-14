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

# client get request
# def get(addr, key):
#     server_address = addr + "/request"
#     payload = {'key': key}
#     message = {"type": "get", "payload": payload}
#     # redirecting till we find the leader, in case of request during election
#     print(redirectToLeader(server_address, message))

def get(addr, tupl):
    server_address = addr + "/request"
    payload = {"pattern": tupl}
    message = {"type": "get", "payload": payload}
    print(redirectToLeader(server_address, message))


if __name__ == "__main__":
    if sys.argv[2] == "get":
        addr = sys.argv[1]
        tupl = tuple(sys.argv[3:])
        print(addr)
        print(tupl)
        get(addr, tupl)
    # if len(sys.argv) == 3:
    #     # addr, key
    #     # get
    #     addr = sys.argv[1]
    #     tupl = sys.argv[2]
    #     get(addr, tupl)
    elif len(sys.argv) == 5:
        # addr, key value
        # put
        addr = sys.argv[1]
        user = sys.argv[2]
        topic = sys.argv[3]
        channel = sys.argv[4]
        put(addr, user, topic, channel)
    else:
        print("PUT usage: python3 client.py address 'key' 'value'")
        print("GET usage: python3 client.py address 'key'")
        print("Format: address: http://ip:port")

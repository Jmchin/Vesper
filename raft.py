#!/usr/bin/env python3

# raft.py

# The beginnings of a raft conesnsus implementation
# Author: Justin Chin

# TODO:
# 1. Implement the distributed log
# 2. Implement Snapshots
# 3. Implement log compaction
# 4. Make election handling more robust


import random
import sys
import time
import threading
import zmq


class Server:
    """The default server in a Raft cluster

    Can be in one of three states at any given time, depending on
    certain conditions of internal state and of the larger raft
    cluster.

    When first initialized, nodes start off in the Follower state, and
    spawn a thread for handling a randomized election timer. If the
    Follower node does not receive a heartbeat response before the
    thread's timer expires, then the node will transistion into the
    Candidate state and solicit it's peers for votes. If the node
    receives a majority of affirmative votes from its peers, then it
    is promoted to Leader and begins sending its own heartbeat.

    ----------------------------------------------------------------------

    Persistent state on all servers:

    current_term : latest TERM server has seen (initialized to 0,
    increases monotonically

    voted_for : candidate_id that received VOTE in current term, or None

    log[] : the log entries, each entry contains a COMMAND for the
    state machine, as well as the term when received by the leader
    (index starts at 1)

    ----------------------------------------------------------------------

    Volatile state on all servers:

    commit_idx : index of the highest log entry known to be committed
    (initialized to 0, increases monotonically)

    last_applied : index of highest log entry applied to state machine
    (initialized to 0, increases monotonically)

    ----------------------------------------------------------------------

    Volatile state on leaders:

    next_idx[] : for each server in the cluster, store the index of
    the next log entry to send to that particular server (initialized
    to leader last log index + 1)

    match_idx[] : for each server in the cluster, index of highest log
    entry known to be replicated on server (initialized to 0,
    increases monotonically)

    """

    def __init__(self, addr, peers):
        self.addr = addr  # 127.0.0.1:5555
        self.peers = peers
        self.state = "follower"

        # Persistent state on ALL servers
        # ----------------------------------------------------------------------
        self.leader = ""
        self.term = 0
        self.voted_for = None
        self.log = []

        # Volatile state on ALL servers
        # ----------------------------------------------------------------------
        self.commit_idx = 0
        self.last_applied = 0
        self.staged = None

        self.vote_count = 0
        self.majority = ((len(peers) + 1) // 2) + 1
        self.timeout_thread = None
        self.election_time = 0
        self.heartbeat_interval = (50 / 100)

        # TRANSPORT
        # ----------------------------------------------------------------------
        self.ctx = zmq.Context()
        self.rep_sock = self.ctx.socket(zmq.REP)
        self.rep_sock.bind(self.addr)
        self.rep_poll = zmq.Poller()
        self.rep_poll.register(self.rep_sock, zmq.POLLIN | zmq.POLLOUT)

        # LEADER STATE
        # ----------------------------------------------------------------------
        self.next_idxs = None
        self.match_idxs = None

        self.follower_loop()

    # UTILITIES
    # ----------------------------------------------------------------------
    def randomize_timeout(self):
        """ Sets server's election time to a random value in [150, 300]
        milliseconds

        """
        # self.election_time = random.randrange(150, 300) / 1000
        self.election_time = random.randrange(150, 300) / 100

    def server_info(self):
        print(f'Server: {self.addr}\nLeader:{self.leader}\nTerm:{self.term}')

    # ELECTION
    # ----------------------------------------------------------------------
    def initialize_election_timer(self):
        """ Spawns an election timer thread, with a randomized timeout

        """
        # print('Initializing election timer')
        # timer has not expired yet, so reinitialize it
        if self.timeout_thread and self.timeout_thread.is_alive():
            self.timeout_thread.cancel()

        self.randomize_timeout()
        self.timeout_thread = threading.Timer(self.election_time,
                                              self.handle_election_timeout)
        self.timeout_thread.start()

    def handle_election_timeout(self):
        """The target function of an election timer thread expiring

        When an election timer expires, the server whose timer expired
        must transition into the Candidate state and begin an election
        by requesting votes from its peer group.

        TODO:

        If election timeout elapses without receiving AppendEntries
        RPC from current leader OR granting vote to candidate: convert
        to candidate

        """
        self.voted_for = None
        if self.state != "leader" and not self.voted_for:
            self.state = "candidate"
            self.term += 1
            self.voted_for = self.addr
            self.vote_count = 1
            self.initialize_election_timer()
            self.request_votes()

    def request_votes(self):
        """ Request votes from every node in the current cluster

        Spawns a thread for each peer in the group. Each thread will
        need to acquire a lock on the main server object, before
        trying to mutate state. Once a majority of votes have been
        received, the server node will transition from the candidate
        state into the leader state and begin sending its own
        heartbeat.

        """
        def request_vote(peer, term):
            # construct a message to send to the peer
            message = {
                "type": "RequestVotes",
                "addr": self.addr,
                "term": self.term,
                "staged": self.staged,
                "commitIdx": self.commit_idx
            }

            print(f'REQUEST_VOTE: {message}')

            # initializes outbound comms socket
            with self.ctx.socket(zmq.REQ) as sock:
                # sock = self.ctx.socket(zmq.REQ)
                sock.setsockopt(zmq.LINGER, 1)
                # connect socket to peer
                sock.connect(peer)
                print(f'connecting to peer: {peer}')

                # poll to see if a send will succeed
                poll = zmq.Poller()
                poll.register(sock, zmq.POLLOUT | zmq.POLLIN)
                events = dict(poll.poll(timeout=5000))

                # ready to send
                if events and events[sock] == zmq.POLLOUT:
                    sock.send_json(message)

                # wait for a response (1 second)
                events = dict(poll.poll(timeout=1000))
                if len(events) > 0 and events[sock] == zmq.POLLIN:
                    resp = sock.recv_json(flags=zmq.NOBLOCK)
                    requester_term = resp["term"]
                    if resp["voteGranted"] and self.state == "candidate":
                        print(f'RECEIVED VOTE GRANTED: {resp}')
                        self.increment_vote()
                    if requester_term > self.term:
                        print("RECEIVED NEWER TERM: Becoming FOLLOWER")
                        self.term = requester_term
                        self.state = "follower"
                        self.follower_loop()
                else:
                    print("Request votes timeout")
                    return False

        for peer in self.peers:
            t = threading.Thread(target=request_vote,
                                 args=(peer, self.term),
                                 daemon=True)
            t.start()

    def handle_request_vote(self, request):
        # from the REP socket, REPLY back to the requestuesting thread
        message = {
            "type": "RequestVotesResponse",
            "addr": self.addr,
            "term": self.term,
            "voteGranted": False
        }

        if self.voted_for and self.term >= request["term"]:
            print(f'NOVOTE:\tself.voted_for = {self.voted_for}\tTerm: {self.term} >= {request["term"]}')
            self.rep_sock.send_json(message)

        # now we need to check whether or not we should grant the vote
        if self.term < request["term"] and self.commit_idx <= request["commitIdx"] and (self.staged == request["staged"]):
            print(f'self.term: {self.term}\trequest.term: {request["term"]}')
            self.initialize_election_timer()
            self.term = request["term"]
            message["voteGranted"] = True
            self.voted_for = request["addr"]
            print(f'VOTING for {request["addr"]} as LEADER for TERM {request["term"]}')

        try:
            self.rep_sock.send_json(message, flags=zmq.NOBLOCK)
        except Exception as e:
            print(e)

    def increment_vote(self):
        if self.state != "leader":
            self.vote_count += 1
            if self.vote_count >= self.majority:
                # become the leader
                print(f'{self.addr} becomes LEADER of term {self.term}')
                self.state = "leader"
                self.leader_loop()

    # BEHAVIOR
    # ----------------------------------------------------------------------

    def follower_loop(self):
        # listen for incoming requests from the leader, or timeout
        while self.state == "follower":
            # time.sleep(1)
            self.initialize_election_timer()
            while self.timeout_thread.is_alive():
                # we are awaiting incoming requests, otherwise we timeout
                if self.rep_poll.poll():
                    req = self.rep_sock.recv_json()

                    if req["type"] == "RequestVotes":
                        # print(f'RECEIVED: {req}')
                        self.handle_request_vote(req)
                    elif req["type"] == "AppendEntries":
                        # print(f'RECEIVED: {req}')
                        self.handle_append_entries(req)
                    elif req["type"] == "InstallSnapshot":
                        # self.handle_install_snapshot()
                        pass

    def leader_loop(self):
        """ The basic event loop for a leader

        """
        # heartbeat
        print("STARTING HEARTBEAT")
        self.server_info()
        for follower in self.peers:
            t = threading.Thread(target=self.append_entries,
                                 args=(follower,))
            t.start()

    def append_entries(self, follower):
        """Leader side AppendEntries RPC

        Invoked by the leader to start it's heartbeat to nodes in the
        cluster. Typically run in its own individual thread once a
        leader comes to power. Sets up a REQ/REP connection with the
        follower, enforcing lock-step communication.

        """
        with self.ctx.socket(zmq.REQ) as sock:
            sock.setsockopt(zmq.LINGER, 1)
            sock.connect(follower)
            poll = zmq.Poller()
            poll.register(sock, zmq.POLLOUT | zmq.POLLIN)
            message = {"type": "AppendEntries",
                       "addr": self.addr,
                       "term": self.term,
                       "commit_idx": self.commit_idx}
            while self.state == "leader":
                # time.sleep(1)
                start_time = time.time()
                events = dict(poll.poll(timeout=1000))
                if events and events[sock] == zmq.POLLOUT:
                    sock.send_json(message)
                if events and events[sock] == zmq.POLLIN:
                    req = sock.recv_json()
                    if req["type"] == "RequestVotes":
                        print(req)
                    elif req["type"] == "RequestVotesResponse":
                        print(req)
                    elif req["type"] == "AppendEntries":
                        print(req)
                    elif req["type"] == "AppendEntriesResponse":
                        print(req)
                    elif req["type"] == "InstallSnapshot":
                        pass
                delta = time.time() - start_time
                time.sleep((self.heartbeat_interval - delta) / 100)

    def handle_append_entries(self, request):
        """ Response handler for AppendEntries request

        This method is invoked on the Follower node in response to the Leader's heartbeat
        """
        self.initialize_election_timer()
        self.term = request["term"]
        self.state = "follower"
        self.leader = request["addr"]
        self.term = request["term"]
        response = {"type": "AppendEntriesResponse", "addr": self.addr, "term": self.term}

        events = dict(self.rep_poll.poll(timeout=1000))
        if events and events[self.rep_sock] == zmq.POLLOUT:
            self.rep_sock.send_json(response)


if __name__ == "__main__":
    # python server.py index ip_list
    if len(sys.argv) == 3:
        index = int(sys.argv[1])
        ip_list_file = sys.argv[2]
        ip_list = []
        # open ip list file and parse all the ips
        with open(ip_list_file) as f:
            for ip in f:
                ip_list.append(ip.strip())
        my_ip = ip_list.pop(index)
        print(f'my_ip: {my_ip}')

        # initialize node with ip list and its own ip
        s = Server(my_ip, ip_list)
    else:
        print("usage: python raft.py <index> <ip_list_file>")

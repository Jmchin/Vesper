#!/usr/bin/env python3

# raft.py

# The beginnings of a raft conesnsus implementation
# Author: Justin Chin

# TODO:
# 1. Implement the distributed log
# 2. Implement Snapshots
# 3. Implement log compaction
# 4. Make election handling more robust
# 5. Look into DEALER-REP pattern for asynchronous leader-client communication


import random
import sys
import time
import threading
import zmq

from tuplespace.proxy import TupleSpaceAdapter


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

    def __init__(self, addr, peers, proxy):
        self.addr = addr  # 127.0.0.1:5555
        self.peers = peers
        self.state = "follower"
        self.lock = threading.Lock()

        # Persistent state on ALL servers
        # ----------------------------------------------------------------------
        self.leader = ""
        self.term = 0
        self.voted_for = None
        self.log = []
        self.proxy = TupleSpaceAdapter(proxy)   # tuplespace proxy uri

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

    # UTILITIES
    # ----------------------------------------------------------------------
    def run(self):
        while True:
            if self.state == "follower":
                self.follower_loop()
            elif self.state == "candidate":
                self.candidate_loop()
            elif self.state == "leader":
                self.leader_loop()

    def randomize_timeout(self):
        """ Sets server's election time to a random value in [150, 300]
        milliseconds

        """
        # self.election_time = random.randrange(150, 300) / 1000
        self.election_time = random.randrange(150, 300) / 100

    def server_info(self):
        print(f'Server: {self.addr}\nLeader:{self.leader}\nTerm:{self.term}')

    def redirect_to_leader(self, request):
        """ Redirects a client request to the current leader of the cluster
        """
        print(f'Request redirection not implemented!')

    # ELECTION
    # ----------------------------------------------------------------------
    def initialize_election_timer(self):
        """ Spawns an election timer thread, with a randomized timeout

        """
        if self.timeout_thread and self.timeout_thread.is_alive():
            self.timeout_thread.cancel()
        self.randomize_timeout()
        self.timeout_thread = threading.Timer(self.election_time,
                                              self.become_candidate)
        self.timeout_thread.start()

    def become_candidate(self):
        self.state = "candidate"

    # def handle_election_timeout(self):
    #     """The target function of an election timer thread expiring

    #     When an election timer expires, the server whose timer expired
    #     must transition into the Candidate state and begin an election
    #     by requesting votes from its peer group.

    #     TODO:

    #     If election timeout elapses without receiving AppendEntries
    #     RPC from current leader OR granting vote to candidate: convert
    #     to candidate

    #     """
    #     self.voted_for = None
    #     if self.state != "leader" and not self.voted_for:
    #         self.state = "candidate"
    #         self.term += 1
    #         self.voted_for = self.addr
    #         self.vote_count = 1
    #         self.initialize_election_timer()
    #         self.request_votes()

    # def request_votes(self):
    #     """ Request votes from every node in the current cluster

    #     Spawns a thread for each peer in the group. Each thread will
    #     need to acquire a lock on the main server object, before
    #     trying to mutate state. Once a majority of votes have been
    #     received, the server node will transition from the candidate
    #     state into the leader state and begin sending its own
    #     heartbeat.

    #     """
    # for peer in self.peers:
    #     t = threading.Thread(target=request_vote,
    #                          args=(peer, self.term))
    #         t.start()

    def request_vote(self, peer, term):
        """Request votes from node in the current cluster

        Spawns a socket for each peer in the group. Each socket will
        need to acquire a lock on the main server object, before
        trying to mutate state. Once a majority of votes have been
        received, the server node will transition from the candidate
        state into the leader state and begin sending its own
        heartbeat.

        """
        # construct a message to send to the peer
        message = {
            "type": "RequestVotes",
            "addr": self.addr,
            "term": self.term,
            "staged": self.staged,
            "commitIdx": self.commit_idx
        }

        print(f'REQUEST_VOTE: {message} to {peer}')

        while self.state == "candidate" and self.term == term:
            # initializes outbound comms socket
            with self.ctx.socket(zmq.REQ) as sock:
                # sock = self.ctx.socket(zmq.REQ)
                # sock.setsockopt(zmq.LINGER, 1)
                # connect socket to peer
                sock.connect(peer)

                # poll to see if a send will succeed
                poll = zmq.Poller()
                poll.register(sock, zmq.POLLOUT | zmq.POLLIN)
                events = dict(poll.poll(timeout=1000))

                # ready to send
                if events and events[sock] == zmq.POLLOUT:
                    sock.send_json(message)

                print(f'sent {message} to {peer}')

                # wait for a response (1 second)
                # TODO: I think this is blocking???
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

    def increment_vote(self):
        if self.state != "leader":
            self.vote_count += 1
            if self.vote_count >= self.majority:
                # become the leader
                print(f'{self.addr} becomes LEADER of term {self.term}')
                self.state = "leader"

    # BEHAVIOR
    # ----------------------------------------------------------------------
    def follower_loop(self):
        # listen for incoming requests from the leader, or timeout
        self.initialize_election_timer()
        while self.state == "follower":
            # we are awaiting incoming requests, otherwise we timeout
            print("in follower loop")
            if self.rep_poll.poll(timeout=1000):
                req = self.rep_sock.recv_json()

                if req["type"] == "RequestVotes":
                    self.handle_request_vote(req)
                elif req["type"] == "AppendEntries":
                    self.handle_append_entries(req)
                elif req["type"] == "InstallSnapshot":
                    pass
                elif req["type"] == "Get":
                    print('follower received get {req}')
                    self.redirect_to_leader(req)
                elif req["type"] == "Put":
                    self.redirect_to_leader(req)

    def candidate_loop(self):
        """ The basic event loop for a candidate

        """
        self.leader = ""
        self.voted_for = None
        votes_granted = 0

        self.initialize_election_timer()
        while self.state == "candidate":
            if not self.voted_for:
                # vote for self and start election
                self.term += 1
                self.voted_for = self.addr
                # send RequestVotes to peers
                for peer in self.peers:
                    threading.Thread(target=self.request_vote,
                                     args=(peer, self.term)).start()

            # listen for incoming requests, only need to respond to
            # AppendEntries from a leader
            events = dict(self.rep_poll.poll(timeout=1000))
            if events and events[self.rep_sock] == zmq.POLLIN:
                req = self.rep_sock.recv_json()
                if req["type"] == "AppendEntries":
                    self.handle_append_entries(req)
                elif req["type"] == "RequestVotes":
                    self.initialize_election_timer()
                    pass

    def leader_loop(self):
        """ The basic event loop for a leader

        """
        # heartbeat
        print("STARTING HEARTBEAT")

        # remove election timeout
        if self.timeout_thread and self.timeout_thread.is_alive():
            self.timeout_thread.cancel()
            self.timeout_thread = None

        # heartbeat
        for follower in self.peers:
            t = threading.Thread(target=self.append_entries,
                                 args=(follower, []))
            t.start()

        # listen for incoming requests
        while self.state == "leader":
            # print("in leader loop")
            # time.sleep(1)
            events = self.rep_poll.poll(timeout=1000)
            if events:
                req = self.rep_sock.recv_json()
                if req["type"] == "RequestVotes":
                    self.handle_request_vote(req)
                elif req["type"] == "AppendEntries":
                    self.handle_append_entries(req)
                elif req["type"] == "InstallSnapshot":
                    pass
                elif req["type"] == "Get":
                    print(req)
                    self.handle_get(req)
                elif req["type"] == "Put":
                    print(req)
                    # self.rep_sock.send_json(req)
                    self.handle_put(req)
                else:
                    print(f"Unknown request: {req}")

    # RPC
    # ----------------------------------------------------------------------

    def handle_request_vote(self, request):
        # from the REP socket, REPLY back to the requesting thread
        message = {
            "type": "RequestVotesResponse",
            "addr": self.addr,
            "term": self.term,
            "voteGranted": False
        }

        # No vote
        if request["term"] < self.term:
            self.rep_sock.send_json(message)
            return

        if (self.voted_for is None or self.voted_for == "addr") and request["commitIdx"] >= self.commit_idx:
            print(f'self.term: {self.term}\trequest.term: {request["term"]}')
            self.initialize_election_timer()
            self.term = request["term"]
            message["voteGranted"] = True
            self.voted_for = request["addr"]
            print(
                f'VOTING for {request["addr"]} as LEADER for TERM {request["term"]}')
        try:
            self.rep_sock.send_json(message, flags=zmq.NOBLOCK)
        except Exception as e:
            print(e)

    def append_entries(self, follower, entries):
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
            while self.state == "leader":
                # time.sleep(1)
                start_time = time.time()

                message = {"type": "AppendEntries",
                           "addr": self.addr,
                           "term": self.term,
                           "entries": entries,
                           "commit_idx": self.commit_idx}

                if self.staged:
                    message["entries"].append(self.staged)

                events = dict(poll.poll(timeout=1000))

                if events and events[sock] == zmq.POLLOUT:
                    sock.send_json(message)
                if events and events[sock] == zmq.POLLIN:
                    res = sock.recv_json()
                    if res["type"] == "AppendEntriesResponse":
                        pass

                delta = time.time() - start_time
                time.sleep((self.heartbeat_interval - delta) / 100)
            print(f'Leaving append_entries thread!')
            return

    def handle_append_entries(self, request):
        """ Response handler for AppendEntries request

        This method is invoked on the Follower node in response to the Leader's heartbeat
        """
        self.initialize_election_timer()

        # build response in advance, mutate it as we go
        response = {"type": "AppendEntriesResponse",
                    "term": self.term,
                    "success": False}

        # leader's term is less than our's §5.1
        if request["term"] < self.term:
            self.rep_sock.send_json(response)

        # TODO:
        # log doesn't contain entry at request["prev_log_idx"] whose
        # term matches request["prev_log_term"] §5.3

        # TODO:
        # if an existing entry conflicts with a new one (same index
        # but different terms), delete the existing entry and all that
        # follow it §5.3

        # TODO:
        # Append any new entries not already in the log

        # TODO:
        # If request["leader_commit"] > commit_idx, commit_idx := min(request["leader_commit"], idx of last new entry)
        self.term = request["term"]
        self.state = "follower"
        self.leader = request["addr"]

        response = {"type": "AppendEntriesResponse",
                    "term": self.term,
                    "success": True}

        print(response)

        events = dict(self.rep_poll.poll(timeout=1000))
        if events and events[self.rep_sock] == zmq.POLLOUT:
            self.rep_sock.send_json(response)

    # LEADER SPECIFIC INVOCATIONS
    # ----------------------------------------------------------------------
    def handle_get(self, request):
        """ Processes a client's GET request

        """
        print(f'Receiving CLIENT GET: {request}')

        pattern = request["payload"]["pattern"]
        result = self.proxy._rdp(pattern)
        self.rep_sock.send_json({"status": 200, "payload": result})

    def handle_put(self, request):
        """ Processes a client's PUT request

        """
        # needs to append to the log, spread to followers, tally
        # acknowledgements, commit once majority reached, spread
        # commit
        print(f'Receiving CLIENT PUT: {request}')
        # time.sleep(5)
        self.staged = request["payload"]

        log_message = {
            "term": self.term,
            "addr": self.addr,
            "payload": request,
            "action": "log",
            "commitIdx": self.commit_idx
        }

        user = request["payload"]["user"]
        topic = request["payload"]["topic"]
        msg = request["payload"]["content"]

        result = self.proxy._out((user, topic, msg))

        ack_count = 0
        self.rep_sock.send_json({"status": 200, "payload": request})
        # spread the update to all followers


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
        line = ip_list.pop(index)
        peers = []

        my_ip = line.split(",")[0]
        proxy_uri = line.split(",")[1]

        print(my_ip)
        proto, host, port = my_ip.split(':')

        for l in ip_list:
            li = l.split(",")
            peers.append(li[0])

        # initialize node with ip list and its own ip
        s = Server(my_ip, peers, proxy_uri)
        s.run()
    else:
        print("usage: python raft.py <index> <ip_list_file>")

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

        self.run()

    # MAIN ENTRY
    # ----------------------------------------------------------------------
    def run(self):
        while True:
            if self.state == "follower":
                self.follower_loop()
            elif self.state == "candidate":
                self.candidate_loop()
            elif self.state == "leader":
                self.leader_loop()

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

    def redirect_to_leader(self, request):
        """ Redirects a client request to the current leader of the cluster
        """
        print(f'Request redirection not implemented!')

    def initialize_election_timer(self):
        """ Spawns an election timer thread, with a randomized timeout

        """
        if self.timeout_thread and self.timeout_thread.is_alive():
            self.timeout_thread.cancel()
        self.randomize_timeout()
        self.timeout_thread = threading.Timer(self.election_time,
                                              self.become_candidate)
        self.timeout_thread.start()

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
        """ The basic event loop for a follower

        """
        # listen for incoming requests from the leader, or timeout
        while self.state == "follower":
            print("in follower loop")
            # time.sleep(1)

            self.initialize_election_timer()

            while self.timeout_thread.is_alive():
                # we are awaiting incoming requests, otherwise we timeout
                events = dict(self.rep_poll.poll())
                if self.rep_sock in events:
                    req = self.rep_sock.recv_json()

                    if req["type"] == "RequestVotes":
                        self.handle_request_vote(req)
                    elif req["type"] == "AppendEntries":
                        self.handle_append_entries(req)
                    elif req["type"] == "Get":
                        self.redirect_to_leader(req)
                    elif req["type"] == "Put":
                        self.redirect_to_leader(req)

    def candidate_loop(self):
        while self.state == "candidate":
            print("in candidate loop")
            self.term += 1
            self.voted_for = self.addr
            self.vote_count = 1

    def leader_loop(self):
        """ The basic event loop for a leader

        """
        # heartbeat
        print("STARTING HEARTBEAT")

        # use a single REQ socket for communication, connect to all followers
        with self.ctx.socket(zmq.REQ) as sock:
            for follower in self.peers:
                pass

                # listen for incoming requests
        while self.state == "leader":
            print("in leader loop")
            if self.rep_poll.poll() == zmq.POLLIN:
                req = self.rep_sock.recv_json()
                if req["type"] == "RequestVotes":
                    self.handle_request_vote(req)
                elif req["type"] == "AppendEntries":
                    self.handle_append_entries(req)
                elif req["type"] == "InstallSnapshot":
                    pass
                    # self.handle_install_snapshot()
                elif req["type"] == "Get":
                    print(req)
                    # self.handle_get(req)
                elif req["type"] == "Put":
                    print(req)
                    self.handle_put(req)
                else:
                    print(f"Unknown request: {req}")

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
                    message["entries"].append(staged)

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
    else:
        print("usage: python raft.py <index> <ip_list_file>")

# raft-server.py

import sys
from raft import Server

# need to respond to GET, PUT, RequestVotes, AppendEntries

# GET
# ----------------------------------------------------------------------
# The GET route is used by a client to retrieve a tuple from the
# replicated data store with some pattern
def get():
    pass


# PUT
# ----------------------------------------------------------------------
# The PUT route is used by a client to write a tuple into the
# replicated data store in the form (user, topic, message)
def put():
    pass


def RequestVotes():
    """RequestVotes RPC -- Invoked by candidates to gather votes

    Arguments:
    ----------------------------------------------------------------------
    term -- candidate's term

    candidate_id -- candidate requesting vote

    last_log_idx -- index of candidate's last log entry

    last_log_term -- term of candidate's last log entry


    Results:
    ----------------------------------------------------------------------
    term -- currentTerm, for candidate to update itself

    vote_granted -- true means candidate received vote


    Receiver Implementation:
    ----------------------------------------------------------------------
    1. Reply false if term < current_term

    2. if voted_for is None or candidate_id, and candidate's log is at
    least as up-to-date as receiver's log, grant vote

    """
    pass

def AppendEntries():
    """AppendEntries RPC -- Invoked by leader to replicate log entries;
    also used as heartbeat

    Arguments:
    ----------------------------------------------------------------------
    term -- leader's term

    leader_id -- so follower can redirect clients

    prev_log_idx -- index of log entry immediately preceding new ones

    prev_log_term -- term of prev_log_index entry

    entries -- log entries to store (empty for heatbeat; may send more
    than one for efficiency)

    leader_commit -- leader's commit_idx


    Results:
    ----------------------------------------------------------------------
    term -- current_term, for leader to update itself

    success -- true if follower contained entry matching prev_log_idx
    and prev_log_term


    Receiver Implementation:
    ----------------------------------------------------------------------
    1. Reply false if term < current_term

    2. Reply false if log doesn't contain an entry at prev_log_idx
    whose term matches prev_log_term

    3. If an existing entry conflicts with a new one (same index but
    different terms), delete the existing entry and all that follow it

    4. Append any new entries not already in the log

    5 If leader_commit > commit_idx, set commit_idx =
    min(leader_commit, index of last new entry)

    """
    pass


if __name__ == '__main__':
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
        fellows = []

        my_ip = line.split(",")[0]
        proxy_uri = line.split(",")[1]

        print(my_ip)
        http, host, port = my_ip.split(':')

        for l in ip_list:
            li = l.split(",")
            fellows.append(li[0])

        # initialize node with ip list and its own ip
        n = Server(fellows, my_ip, proxy_uri)

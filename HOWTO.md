# HOWTO

## Dependencies

``` sh
pip3 install --user flask
```

## Setup

1. Navigate to `/tuplespace` directory

    `cd tuplespcae`

2. Initialize the tuplespaces and their adapters

   `foreman start -f Procfile_TS`

3. Navigate up one directory

    `cd ..`

4. Open 5 separate terminal windows

5. Spin up a raft server instance in each terminal

   `python3 server.py {idx} ip_list.txt`

   idx is an integer in range [0,4], and should be distinct for each
   invocation

6. Verify that one of the terminal's raft servers has started a
   HEARTBEAT and become the cluster leader

7. Invoke the client to write a tuple to the raft cluster

    `python3 client.py {protocol}://{address}:{port} {put | get} [-u]
    {user} [-t] {topic} [-m] "{message}`

    `python3 client.py http://127.0.0.1:5000 put -u alice -t
    distsys -m "Hello, world!"`

    The port number should be one of the port numbers on the address
    that is bound to a raft HTTP server. Valid values will depend on
    invocation, however, following this document will produce a valid
    port range of [5000, 5004].

    The -u, -t and -m flags denote a user, topic and message
    respectively. All three must be present for a well-formatted PUT
    request, while any combination of the three is allowable for a GET
    request.

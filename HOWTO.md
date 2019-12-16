# HOWTO

## Dependencies

``` sh
pip3 install --user flask
```

## Setup

1. Navigate to `/tuplespace` directory

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

    `python3 client.py {protocol}://{addr}:{port} put -u alice -t
    distsys -m "Hello, world!"`
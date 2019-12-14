# Project II - cpsc551

## Approach 1 - Naming, Delivery & Recovery

1. Start up the recover and naming servers

    `./approach1.sh`

2. Initialize client tuplespace/adapter pairs

    `foreman start -f Procfile_TS`

3. Write to tuplespace

    `./mblog.py <USER> <TOPIC> "<MESSAGE>"`

4. Verify message was written

    `./workshop.py -c <USER_2>`

    `>>> ts._rdall((str, str, str))`

5. Bring new user online

    `foreman start -f Procfile_TS`

6. Verify recovery

    `./workshop.py -c dave.yaml`

    `>>> ts._rdall((str, str, str))`


## Approach 2 - Replication

1. Start up tuplespace, adapter, and manager

    `foreman start -f Procfile_TSM`

2. Write something to a tuplespace

    `./workshop.py -c alice.yaml`

    `ts._out(('alice', 'distsys', 'hello, world'))`

3. Observe the events bouncing between all the tuplespaces

4. Attempt to bring Dave to life

    These steps were separated to avoid timing conflicts where the manager was attempting to connect to a un-ready adapter.

    `foreman start -f Profile_NewTS`

    `./start_dave_tsm.sh`

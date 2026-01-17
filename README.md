# Gossip Glomers Challenges

My solutions to the series of distributed systems challenges [Gossip Glomers](https://fly.io/dist-sys/).
Each challenge has its own directory and can be run with provided scripts, assuming [Maelstrom](https://github.com/jepsen-io/maelstrom) is available in its own directory in the root of this project.
In the most basic scenarios here, the distributed system consists of nodes exchaning messages between each other. Each node can have a number of clients making various requests.

## Challenge #1: Echo
Basic message exchange for verifying that the setup works correctly.

## Challenge #2: Unique ID Generation
Version 7 UUIDs are generated for each node based on current time and random values to guarantee ID uniqueness.

## Challenge #3a: Single-Node Broadcast
Basic setup for further challenges. Various *message types* are handled. Values *broadcast*ed to the node by clients are stored locally and can be then *read*. *topology* is irrevelant here and so it's ignored.

## Challenge #3b: Multi-Node Broadcast
Each node is meant to propagate all values it has received as a *broadcast* to all other nodes in the network. The provided *topology* is utilized to establish the neighbours of each node. The values are unique, so the nodes can utilize a local set of all received values - if the value is present in the set, then the node doesn't *broadcast* it any further. The set has to be operated in conjuction with a mutex, as multiple *broadcast*s can be handled at the same time by a node, same with *read*s.
"read" is only used for checking the correctness of the solution.

## Challenge #3c: Fault Tolerant Broadcast
This broadcast system is meant to handle temporary network partitions between nodes. For this purpose, each value meant to be *broadcast*ed gets its own goroutine, which repeatedly attempts a synchronous request with a short timeout until success (until the network partition is cleared).

## Challenge #3d: Efficient Broadcast, Part I
Here the network partitions are still present and a latency of 100ms is introduced to each sent message.
The goal is to achieve the following metrics:
- Messages-per-operation is below 30
- Median latency is below 400ms
- Maximum latency is below 600ms 

Messages-per-operation is the average number of messages exchanged between nodes (listed as servers by Maelstrom) per a request from a client.
To achieve these results, the provided topology has to be ignored (the imposed 100ms latency being the main culprit here). I assumed a star topology, i.e. one node is the only neighbour of all the other nodes. 
Besides that, I changed the synchronous request to have a timeout of increasing duration with each failure, to hopefully reduce the number of exchanged messages due to network partitions.

With that I have achieved the following results:
- Messages-per-operation: ~22
- Median latency: ~170ms
- Maximum latency: ~220ms

## Challenge #3e: Efficient Broadcast, Part II
Here the conditions are the same, but the challenge is to decrease messages-per-operation while sacrificing latencies, to achieve the following metrics:
- Messages-per-operation is below 20
- Median latency is below 1 second
- Maximum latency is below 2 seconds 

For that purpose I've introduced a local buffer with its own mutex to each node, which accumulates the values *broadcast*ed by clients. The *broadcast* itself no longer triggers further *broadcast*s and value propagation, instead that is now handled by a goroutine loop which intermittenly broadcasts the buffered values in form of a list to all of the neighbours (same topology as previously). Same mechanism with increased timeouts is still used in separate goroutines.

That allowed me to achieve the following results:
- Messages-per-operation: ~8.5
- Median latency: ~250ms
- Maximum latency: ~390ms

## Challenge #4: Grow-Only Counter 
The goal is to create a distributed counter using a provided sequentially-consistent key/value storage.
The nodes are supposed to handle *read*s which should return the current value of the counter and *add*, which increment the counter by the provided delta.

For *add*s I'm using compare-and-swap (CAS) to ensure the counter is properly incremented. The first CAS is based on a local cache to reduce the average number of exchanged messages per operation. If it fails, then the node repeatedly keeps reading the counter and attempting CAS until success, updating the cache in the end. Naturally, *read* also updates the cache.
The *read* has to write to a different key in the storage before actually reading the counter value, so as to ensure that the final *read* (which is what mostly matters here), is not reordored before any CASes of other nodes and actually returns the final recorded value.

## Challenge #5a: Single-Node Kafka-Style Log 
Here we're simply preparing the setup for further challenges.
Each value under a key received via *send* is supposed to have an offset assigned to it, that's unique per key. The log is stored locally together with a mutex.
*poll* comes with key-offset pairs - for each key we're supposed to return all offset-value pairs starting from the given offset.
With *commit_offsets* all the given keys are supposed to be considered up to the given offsets. Committed offsets are stored separately with a mutex. *list_committed_offsets* lists committed offsets for each given key.

## Challenge #5b: Multi-Node Kafka-Style Log 
We have to transition to a setup, where the local storage of both keys-values-offsets and committed offsets is moved to a shared, linearizable storage. CASes are used for *send* and *commit_offsets*. The average messages per operation ended up at 7.4.

## Challenge #5c: Efficient Kafka-Style Log
Here we try to improved 5b as much as we can.
In 5C1 I've tried playing around with a local cache for the log, since the storage is supposed to be distributed only among two nodes and there was a minor improvement down to an average of 7.3.
In 5C2 I've used consistent 'hashing' of keys (which can be easily done via modulo, since the keys are just integers) and distributed them among the nodes. That allowed me to replace the CASes with writes and decrease messages per operation down to an average of 6.8.
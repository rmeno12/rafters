# rafters

TODO:
- implement grpc raft server for raftserver binary
- do consensus raft things
  - add necessary inter-raft server grpc comms
  - implement actual raft logic
  - notify frontend on leader/term changes
- implement get/put/replace in frontend to interact with leader

desired features for midpoint:
- leader election
- basic consensus with no failures
- get/put/replace functionality

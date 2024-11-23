# rafters

TODO:
- implement frontend things
- don't forward to leader, respond with wrong leader and leader id
  - how to handle retry?
- test failures:
  - fail non-leaders
  - fail leader
  - partition/cut connections between raft nodes
- make things work with ta testing code

questions for oh:
- how to test rigorously?
- what are reply fields for?

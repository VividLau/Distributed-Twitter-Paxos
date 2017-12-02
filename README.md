# Distributed-Twitter-Paxos
Using simplified Paxos algorithm to maintain logs in distributed twitter systems.

***Note:***  
The leader election mechanism and the accpt(0, v) in some case will lead to the following situation:  
In proposer receives accVal from acceptors:  
0, V1  
0, V1  
0, None  
0, None  
0, None  
in this case, if the proposer choose accVal according to the max accNum, it will lead to uncertainty.

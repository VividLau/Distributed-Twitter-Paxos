# Distributed-Twitter-Paxos
Using simplified Paxos algorithm to maintain logs in distributed twitter systems.

***Bugs:***  
No bug for now, try to find more corner case.  
  
***Note:***  
Now the log is writed and read as a whole. Consider the amount of data in this project, it's fine to do that.  
However, obviously it is inefficient. Later would try not to load it as a whole, and just maintain the in-memory data structure for timeline.

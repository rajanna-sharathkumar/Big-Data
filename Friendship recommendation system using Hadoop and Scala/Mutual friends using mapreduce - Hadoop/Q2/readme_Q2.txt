Q2.
Please answer this question by using dataset from Q1.
Given any two Users as input, output the list of the user id of their mutual friends.
Output format:
UserA, UserB list userid of their mutual Friends.


How to run:
Hadoop jar <jar file location> MutualFriends.mf.MutualFriends <user1> <user2> <input dataset location> <output location>

Example:
> hadoop jar hadoop_files/mf-0.0.1-SNAPSHOT.jar MutualFriends.mf.MutualFriends 0 1 /user/sharath/hw1/input/h1.txt /user/sharath/hw1/output2/
> cat /user/sharath/hw1/output2/*
0:1     20,5

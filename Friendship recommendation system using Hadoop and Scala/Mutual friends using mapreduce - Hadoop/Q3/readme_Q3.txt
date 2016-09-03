Please use in-memory join to answer this question.
Given any two Users as input, output the list of the names and the zipcode of their mutual friends.
Note: use the userdata.txt to get the extra user information.
Output format:
UserA id, UserB id, list of [names:zipcodes] of their mutual Friends.
Sample Output:
1234 4312 [John:75075, Jane : 75252, Ted:45045]


How to run?

Hadoop jar <jar file location> <class name> <user1> <user2> <socnet dataset location> <inmemory file location> <userdata dataset location> <output location>

Example:

[mapr@maprdemo ~]$ hadoop jar hadoop_files/jobchain-0.0.1-SNAPSHOT.jar InMemoryJoin 0 1 /user/sharath/hw1/input/h1.txt /user/sharath/hw1/output3_1/ /user/sharath/hw1/input/userdata.txt /user/sharath/hw1/output3/

[mapr@maprdemo ~]$ cat /user/sharath/hw1/output3/*
0:1     [Juan:29201,Beth:33463]

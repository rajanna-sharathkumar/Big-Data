Q1
Write a MapReduce program in Hadoop that implements a simple “People You
Might Know" social network friendship recommendation algorithm. The key idea
is that if two people have a lot of mutual friends, then the system should recommend that
they connect with each other.
Input:
Input files
1. soc-LiveJournal1Adj.txt located in /socNetData/networkdata in hdfs on cs6360 cluster
The input contains the adjacency list and has multiple lines in the following
format:
<User><TAB><Friends>
2. userdata.txt located in /socNetData/userdata in hdfs on cs6360 cluster
The userdata.txt contains dummy data which consist of
column1 : userid
column2 : firstname
column3 : lastname
column4 : address
column5: city
column6 :state
column7 : zipcode
column8 :country
column9 :username
column10 : date of birth.
Here, <User> is a unique integer ID corresponding to a unique user and <Friends> is a
comma-separated list of unique IDs corresponding to the friends of the user with the
unique ID <User>. Note that the friendships are mutual (i.e., edges are undirected): if A
is friend with B then B is also friend with A. The data provided is consistent with that rule
as there is an explicit entry for each side of each edge.
Algorithm: Let us use a simple algorithm such that, for each user U, the algorithm
recommends N = 10 users who are not already friends with U, but have the largest
number of mutual friends in common with U. Note that you are to use only the second
level friendship to find mutual friends.
Output: The output should contain one line per user in the following format:
<User><TAB><Recommendations>
where <User> is a unique ID corresponding to a user and <Recommendations> is a
comma-separated list of unique IDs corresponding to the algorithm's recommendation of
people that <User> might know, ordered by decreasing number of mutual friends. Even if
a user has fewer than 10 second-degree friends, output all of them in decreasing order of
the number of mutual friends. If a user has no friends, you can provide an empty list of
recommendations. If
there are multiple users with the same number of mutual friends, ties are broken by
ordering them in a numerically ascending order of their user IDs.


How to Run:
Hadoop jar <JarFile location> PeopleYouKnow <input dataset location> <Output destination>

few_reccomendations.txt conatians for the users with following user ID's:
924, 8941, 8942, 9019, 9020, 9021, 9022, 9990, 9992, 9993.
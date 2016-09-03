Q4.
Using reduce-side join and job chaining:
Step 1: Calculate the average age of the direct friends of each user.
Step 2: Sort the users by the calculated average age from step 1 in descending order.
Step 3. Output the top 20 users from step 2 with their address and the calculated average age.
Sample output.

User A, 1000 Anderson blvd, Dallas, TX, average age of direct friends.


How to run?
hadoop jar <jar file location> <userdata location> <socdata location> <userdata location> <output location> <output location>

intermediate output will be stored in <output location>_int1
Final output can be found in <output lcoation>_int2


Example:

[mapr@maprdemo ~]$ hadoop jar hadoop_files/AverageAgeCalc-0.0.1-SNAPSHOT.jar AverageAge /user/sharath/hw1/input/userdata.txt /user/sharath/hw1/input/h1 /user/sharath/hw1/input/userdata.txt /user/sharath/hw1/output4_1 /user/sharath/hw1/output4_1

[mapr@maprdemo ~]$ cat /user/sharath/hw1/output4_1_int2/*
Jeane   Jeane,69,4999 Bee Street,Traverse City,Michigan,86.0
Amanda  Amanda,44,1460 Rose Street,Harvey,Illinois,86.0
Dan     Dan,41,3057 Elmwood Avenue,Scottsdale,Arizona,86.0
Mary    Mary,35,1833 Boring Lane,San Francisco,California,86.0
Richard Richard,36,4287 Locust Street,Albany,Georgia,86.0
Kathleen        Kathleen,44,2741 Timber Ridge Road,Sacramento,California,86.0
Ron     Ron,19,3848 Ritter Avenue,Utica,Michigan,85.0
Filiberto       Filiberto,23,131 Jett Lane,Los Angeles,California,85.0
Richard Richard,63,746 Braxton Street,Prophetstown,Illinois,85.0
Jennifer        Jennifer,41,152 Snowbird Lane,Henderson,Nebraska,85.0
Darla   Darla,60,1024 Graystone Lakes,Cedar Grove,Georgia,85.0
Leonard Leonard,43,4449 Kildeer Drive,Hampton,Virginia,85.0
Diane   Diane,58,3810 Willis Avenue,Deltona,Florida,85.0
Joe     Joe,19,9 Upland Avenue,Sylvania,Ohio,85.0
Carroll Carroll,58,1606 Briarhill Lane,Akron,Ohio,85.0
Trevor  Trevor,78,187 Charla Lane,Plano,Texas,85.0
Maureen Maureen,54,568 Columbia Road,Denver,Colorado,85.0
Arleen  Arleen,64,3277 Glendale Avenue,Los Angeles,California,85.0
John    John,30,2433 Kinney Street,Springfield,Massachusetts,85.0
Annie   Annie,48,1461 John Calvin Drive,Chicago,Illinois,85.0

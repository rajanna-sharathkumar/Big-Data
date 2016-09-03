movieData = LOAD '/Spring-2016-input/movies.dat' USING PigStorage(':') AS (MovieID:int,Title:chararray,Genres:chararray);
ratingsData = LOAD '/Spring-2016-input/ratings.dat' USING PigStorage(':') AS (UserID:int,MovieID:int,Rating:double,Timestamp:chararray);
userData = LOAD '/Spring-2016-input/users.dat' USING PigStorage(':') AS (UserID:int,Gender:chararray,Age:int,Occupation:chararray,Zipcode:chararray);

actionAndWar = FILTER movieData BY Genres matches '.*Action.*' and Genres matches '.*War.*';
joinedData = JOIN actionAndWar BY MovieID, ratingsData BY MovieID;
groupedData = GROUP joinedData BY actionAndWar::MovieID;
avgData = FOREACH groupedData GENERATE group AS MovieID, AVG(joinedData.ratingsData::Rating) AS average_rating;

femaleUser = FILTER userData BY Gender eq 'F';
femaleUserAge = FILTER femaleUser BY Age > 23 and Age < 35 AND Zipcode matches '1.*';
ratingsOfFemaleUsers = JOIN ratingsData BY UserID, femaleUserAge BY UserID;

usersWhoRatedLow = JOIN ratingsOfFemaleUsers BY MovieID, avgData BY MovieID;
filtered_output = FILTER usersWhoRatedLow BY ratingsOfFemaleUsers::ratingsData::Rating < avgData::average_rating;
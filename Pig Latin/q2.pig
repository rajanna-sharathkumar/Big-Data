RATING = LOAD '/Spring-2016-input/ratings.dat' USING PigStorage(':') AS (userid,movieid,rating);
MOVIE = LOAD '/Spring-2016-input/movies.dat' USING PigStorage(':') AS (movieid:int,title:chararray,genre:chararray);
grouped = COGROUP MOVIE BY movieid, RATING BY movieid;  
first5 = LIMIT grouped 5; 
DUMP first5;                 
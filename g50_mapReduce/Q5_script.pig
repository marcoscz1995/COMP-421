movies = LOAD '/data/movies.csv' USING PigStorage(',') AS (movieid:INT, title:CHARARRAY, year:INT); 
ratings = LOAD '/data/ratings.csv' USING PigStorage(',') AS (userid:INT, movieid:INT, rating:DOUBLE, TIMESTAMP); 

movieratings = JOIN movies BY movieid, ratings BY movieid;

movieratingcountraw = GROUP movieratings BY movies::movieid;

movieratingcountrawrenamed = FOREACH movieratingcountraw GENERATE group AS movieid:INT , COUNT(movieratings) AS numratings:LONG;

movieratingcount = ORDER movieratingcountrawrenamed BY numratings DESC;

dump movieratingcount;

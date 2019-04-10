movies = LOAD '/data/movies.csv' USING PigStorage(',') AS (movieid:INT, title:CHARARRAY, year:INT); 
ratings = LOAD '/data/ratings.csv' USING PigStorage(',') AS (userid:INT, movieid:INT, rating:DOUBLE, TIMESTAMP);
moviegenres = LOAD '/data/moviegenres.csv' USING PigStorage(',') AS (movieid:INT, genre:CHARARRAY);

genrejoin = JOIN movies BY movieid, moviegenres BY movieid;

moviesscifi2015 = FILTER genrejoin BY (year == 2015) AND (genre == 'Sci-Fi');

movieratings = JOIN moviesscifi2015 BY movies::movieid, ratings BY movieid;

movieratingcountraw = GROUP movieratings BY movies::movieid;

movieratingcountrawrenamed = FOREACH movieratingcountraw GENERATE group AS movieid:INT , COUNT(movieratings) AS numratings:LONG;

movieratingcountrawordered = ORDER movieratingcountrawrenamed BY numratings DESC;

movieratingcount = LIMIT movieratingcountrawordered 5;

dump movieratingcount;
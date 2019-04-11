movies = LOAD '/data/movies.csv' USING PigStorage(',') AS (movieid:INT, title:CHARARRAY, year:INT); 
ratings = LOAD '/data/ratings.csv' USING PigStorage(',') AS (userid:INT, movieid:INT, rating:DOUBLE, TIMESTAMP);
moviegenres = LOAD '/data/moviegenres.csv' USING PigStorage(',') AS (movieid:INT, genre:CHARARRAY);

movies2016 = FILTER movies BY year == 2016;

genrejoin = JOIN movies2016 BY movieid, moviegenres BY movieid;

ratingsjoin = JOIN movies2016 BY movieid, ratings BY movieid;

genrecount = GROUP genrejoin BY movies2016::movieid;

ratingscount = GROUP ratingsjoin BY movies2016::movieid;

moviesandcounts = JOIN genrecount BY group, ratingscount BY group;

moviestatsintermediate = FOREACH moviesandcounts GENERATE genrecount::group AS movieid:INT , COUNT(ratingsjoin) AS ratingCount:LONG , COUNT(genrejoin) AS genreCount:LONG;

moviestats = JOIN moviestatsintermediate BY movieid, movies2016 BY movieid;

movieattributecounts = FOREACH moviestats GENERATE moviestatsintermediate::movieid AS movieid:INT , title AS title:CHARARRAY , genreCount AS genreCount:LONG, ratingCount AS ratingCount:LONG;

STORE movieattributecounts INTO 'q7' USING PigStorage (',') ; 
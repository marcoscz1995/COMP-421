movies = LOAD '/data/movies.csv' USING PigStorage(',') AS (movieid:INT, title:CHARARRAY, year:INT);
moviegenres = LOAD '/data/moviegenres.csv' USING PigStorage(',') AS (movieid:INT, genre:CHARARRAY);

movies1516 = FILTER movies BY (year == 2015) OR (year == 2016);

movies1516genres = JOIN movies1516 BY movieid, moviegenres BY movieid;

moviesgrouped = GROUP movies1516genres BY genre;

genrecount = FOREACH moviesgrouped GENERATE group AS genre:CHARARRAY , COUNT(movies1516genres) AS count:LONG;

dump genrecount;
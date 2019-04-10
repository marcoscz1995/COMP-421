movies = LOAD '/data/movies.csv' USING PigStorage(',') AS (movieid:INT, title:CHARARRAY, year:INT);
moviegenres = LOAD '/data/moviegenres.csv' USING PigStorage(',') AS (movieid:INT, genre:CHARARRAY);

movies2015 = FILTER movies BY year == 2015;

desiredgenres = FILTER moviegenres BY (genre == 'Comedy') OR (genre == 'Sci-Fi');

movies2015Genres = JOIN movies2015 BY movieid, desiredgenres BY movieid PARALLEL 4;

movies15Gtitles = FOREACH movies2015Genres GENERATE title AS title:CHARARRAY;

movies15GtitlesDIST = DISTINCT movies15Gtitles;

movies15gtitlesDISTORDERED = ORDER movies15GtitlesDIST BY title ASC;

dump movies15gtitlesDISTORDERED;




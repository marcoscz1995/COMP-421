
--load the data from HDFS and define the schema
movies = LOAD '/data/movies.csv' USING PigStorage(',') AS (movieid:INT, title:CHARARRAY, year:INT);

-- group movies by year
moviesperyear = GROUP movies BY year;

-- Read only the attributes we are interested in.
yearcount = FOREACH moviesperyear GENERATE group AS year:INT , COUNT(movies) AS count:LONG;

-- Order that by year.
STORE (ORDER yearcount BY year ASC) INTO 'q1' USING PigStorage (',') ;


--load the data from HDFS and define the schema
movies = LOAD '/data/movies.csv' USING PigStorage(',') AS (movieid:INT, title:CHARARRAY, year:INT);

--load the data from HDFS and define the schema
airports = LOAD '/data/airports.csv' USING PigStorage(',') AS (airport_id:INT, airport_code:CHARARRAY, city_name:CHARARRAY, state:CHARARRAY);

-- group movies by year
moviesperyear = GROUP movies BY year;

-- group airports by state
groupbyState = GROUP airports BY state;

-- Read only the attributes we are interested in.
airportcount = FOREACH groupbyState GENERATE group AS state:CHARARRAY, COUNT(airports) AS count:LONG;


-- Read only the attributes we are interested in.
yearcount = FOREACH moviesperyear GENERATE group AS year:INT , COUNT(movies) AS count:LONG;

-- Order that by year.
STORE (ORDER yearcount BY year ASC) INTO 'q1' USING PigStorage (',') ;

-- Order that by state.
STORE (ORDER airportcount BY state ASC) INTO 'q1' USING PigStorage (',') ;

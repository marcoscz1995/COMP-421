movies = LOAD '/data/movies.csv' USING PigStorage(',') AS (movieid:INT, title:CHARARRAY, year:INT); 
ratings = LOAD '/data/ratings.csv' USING PigStorage(',') AS (userid:INT, movieid:INT, rating:DOUBLE, TIMESTAMP);
moviegenres = LOAD '/data/moviegenres.csv' USING PigStorage(',') AS (movieid:INT, genre:CHARARRAY);

airports = LOAD '/data/airports.csv' USING PigStorage(',') AS (airport_id:INT, airport_code:CHARARRAY, city_name:CHARARRAY, state:CHARARRAY);
airplanes = LOAD '/data/airplanes.csv' USING PigStorage(',') AS (carrier_code:CHARARRAY, carrier_name:CHARARRAY, tail_number:CHARARRAY);
flights = LOAD '/data/flights.csv' USING PigStorage(',') AS (day:INT, flight_number:CHARARRAY, tail_number:CHARARRAY, origin_airport_id:INT, dest_airport_id:INT, delay:INT, distance:INT);

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

--filter
delta = FILTER airplanes BY carrier_code == DL;
jfklaxids = FILTER airports BY (airport_code == 'JFK') OR (airport_code == 'LAX');

--join
--get the flights under delta airlines
deltajoin = JOIN delta BY tail_number, flights BY tail_number;
--get the delta airline flights that originate in either jkf or lax
jfklaxjoinOrigin = JOIN jfklaxids BY airport_id, deltajoin BY origin_airport_id; 
--get the delta airline flights that arrive in either jkf or lax
jfklaxjoinDest = JOIN jfklaxids BY airport_id, deltajoin BY dest_airport_id; 
--find the flights that commute only between jkf and lax
jfklax = JOIN jfklaxjoinOrigin BY (origin_airport_id, dest_airport_id) LEFT OUTER, jfklaxjoinDest BY (origin_airport_id, dest_airport_id); 

--cut the fat ie project now to save on time
skinny = FOREACH jfklax GENERATE tail_number, distance;

--group and sum
jfklax_group = GROUP skinny BY tail_number;
airplane_dist = FOREACH jfklax_group GENERATE group AS tail_number, SUM(flights.distance) AS totoaldistance;

--order by totoaldistance and then tail number, limit to 10
airplane_dist_ordered = ORDER airplane_dist BY totoaldistance DESC, tail_number DESC;
limit_data = LIMIT airplane_dist_ordered 10;

--project only tail number and distance
airplaneDist = FOREACH limit_data GENERATE group as tail_number:CHARARRAY, totoaldistance AS totoaldistance:INT;


--dump the results
dump airplaneDist;

--store the data
STORE airplaneDist INTO 'q7' USING PigStorage (',') ; 

--explain

EXPLAIN airplaneDist;








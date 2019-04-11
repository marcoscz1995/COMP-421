airports = LOAD '/data/airports.csv' USING PigStorage(',') AS (airport_id:INT, airport_code:CHARARRAY, city_name:CHARARRAY, state:CHARARRAY);
airplanes = LOAD '/data/airplanes.csv' USING PigStorage(',') AS (carrier_code:CHARARRAY, carrier_name:CHARARRAY, tail_number:CHARARRAY);
flights = LOAD '/data/flights.csv' USING PigStorage(',') AS (day:INT, flight_number:CHARARRAY, tail_number:CHARARRAY, origin_airport_id:INT, dest_airport_id:INT, delay:INT, distance:INT);

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









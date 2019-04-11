airports = LOAD '/data/airports.csv' USING PigStorage(',') AS (airport_id:INT, airport_code:CHARARRAY, city_name:CHARARRAY, state:CHARARRAY);
airplanes = LOAD '/data/airplanes.csv' USING PigStorage(',') AS (carrier_code:CHARARRAY, carrier_name:CHARARRAY, tail_number:CHARARRAY);
flights = LOAD '/data/flights.csv' USING PigStorage(',') AS (day:INT, flight_number:CHARARRAY, tail_number:CHARARRAY, origin_airport_id:INT, dest_airport_id:INT, delay:INT, distance:INT);

--filter
delta = FILTER airplanes BY carrier_code == 'DL';
jfklaxids = FILTER airports BY (airport_code == 'JFK') OR (airport_code == 'LAX');
jfklax_orig = FILTER flights BY (origin_airport_id == 12892) OR (origin_airport_id == 12478);
jfklax_both = FILTER jfklax_orig BY (dest_airport_id == 12892) OR (dest_airport_id == 12478);

--join
--ge the delta lfights that coommute between jfk and lax that are delta
deltajoin = JOIN delta BY tail_number, jfklax_both BY tail_number;

jfklax10 = limit jfklax 10;
dump jfklax10;


--the first four fields have values, the rest dont. why?

ori = FOREACH jfklax GENERATE jfklaxjoinOrigin::tail_number as tail_number:chararray, jfklaxjoinOrigin::distance as or_distance:int, 
	jfklaxjoinOrigin::origin_airport_id ,
	jfklaxjoinOrigin::dest_airport_id;
dest = FOREACH jfklax GENERATE jfklaxjoinDest::tail_number as tail_number:chararray, 
	jfklaxjoinDest::distance as dest_distance:int, 
	jfklaxjoinDest::origin_airport_id ,
	jfklaxjoinDest::dest_airport_id;

ori_group = GROUP ori BY tail_number;
dest_group = GROUP dest BY tail_number;

flight_dist = JOIN ori_group BY group, dest_group BY group;




ori10 = limit ori 10;
dest10 = limit dest 10;

dump ori10;
dump dest10; why is this empty?


--cut the fat ie project now to save on time
skinny = FOREACH jfklax GENERATE jfklaxjoinOrigin::tail_number as tail_number:chararray, jfklaxjoinOrigin::distance as or_distance:int, 
	jfklaxjoinDest::distance as des_distance:int;

--combing origin and destination distances
adds = FOREACH skinny GENERATE tail_number, (or_distance + des_distance) as distance;

--group and sum
jfklax_group = GROUP jfklax BY tail_number;
airplane_dist = FOREACH jfklax_group GENERATE group AS tail_number, SUM(distance) AS totoaldistance:INT;

--order by totoaldistance and then tail number, limit to 10
airplane_dist_ordered = ORDER airplane_dist BY totoaldistance DESC, tail_number DESC;
limit_data = LIMIT airplane_dist_ordered 10;

--project only tail number and distance
airplaneDist = FOREACH limit_data GENERATE group as tail_number:CHARARRAY, totoaldistance AS totoaldistance:INT;


--dump the results
dump airplaneDist;

--store the data
STORE airplaneDist INTO 'q7' USING PigStorage (',') ; 









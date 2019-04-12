airports = LOAD '/data/airports.csv' USING PigStorage(',') AS (airport_id:INT, airport_code:CHARARRAY, city_name:CHARARRAY, state:CHARARRAY);
airplanes = LOAD '/data/airplanes.csv' USING PigStorage(',') AS (carrier_code:CHARARRAY, carrier_name:CHARARRAY, tail_number:CHARARRAY);
flights = LOAD '/data/flights.csv' USING PigStorage(',') AS (day:INT, flight_number:CHARARRAY, tail_number:CHARARRAY, origin_airport_id:INT, dest_airport_id:INT, delay:INT, distance:INT);

--filter
delta = FILTER airplanes BY carrier_code == 'DL';
jfklaxids = FILTER airports BY (airport_code == 'JFK') OR (airport_code == 'LAX');
jfklax_orig = FILTER flights BY (origin_airport_id == 12892) OR (origin_airport_id == 12478);
jfklax_both = FILTER jfklax_orig BY (dest_airport_id == 12892) OR (dest_airport_id == 12478);

--join
--get the delta fights that coommute between jfk and lax that are delta
deltajoin = JOIN delta BY tail_number, jfklax_both BY tail_number;

--project
jfklax = FOREACH deltajoin GENERATE delta::tail_number AS tail_number:CHARARRAY, jfklax_both::distance AS distance:INT;

--group and sum
jfklax_group = GROUP jfklax BY tail_number;
airplane_dist = FOREACH jfklax_group GENERATE group AS tail_number, SUM(jfklax.distance) AS totoaldistance:INT;

--order by totoaldistance and then tail number, limit to 10
airplane_dist_ordered = ORDER airplane_dist BY totoaldistance DESC, tail_number DESC;
limit_data = LIMIT airplane_dist_ordered 10;

--dump the results
dump limit_data;

--store the data
STORE limit_data INTO 'q7' USING PigStorage (',') ; 









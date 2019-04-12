airports = LOAD '/data/airports.csv' USING PigStorage(',') AS (airport_id:INT, airport_code:CHARARRAY, city_name:CHARARRAY, state:CHARARRAY);
airplanes = LOAD '/data/airplanes.csv' USING PigStorage(',') AS (carrier_code:CHARARRAY, carrier_name:CHARARRAY, tail_number:CHARARRAY);
flights = LOAD '/data/flights.csv' USING PigStorage(',') AS (day:INT, flight_number:CHARARRAY, tail_number:CHARARRAY, origin_airport_id:INT, dest_airport_id:INT, delay:INT, distance:INT);

--condition
flightdelay600 = FILTER flights BY delay > 600;

--join
flightdelay600carriercodes = JOIN flightdelay600 BY tail_number, airplanes BY tail_number PARALLEL 4 ;

--project
flightcarriercodes = FOREACH flightdelay600carriercodes GENERATE carrier_code AS carrier_code:CHARARRAY;

--get distinct values
codeDistinct = DISTINCT flightcarriercodes;

codeDistinctOrdered = ORDER codeDistinct by carrier_code ASC;

--dump

dump codeDistinctOrdered;


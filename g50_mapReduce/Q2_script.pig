movies = LOAD '/data/movies.csv' USING PigStorage(',') AS (movieid:INT, title:CHARARRAY, year:INT);
moviegenres = LOAD '/data/moviegenres.csv' USING PigStorage(',') AS (movieid:INT, genre:CHARARRAY);

airports = LOAD '/data/airports.csv' USING PigStorage(',') AS (airport_id:INT, airport_code:CHARARRAY, city_name:CHARARRAY, state:CHARARRAY);
airplanes = LOAD '/data/airplanes.csv' USING PigStorage(',') AS (carrier_code:CHARARRAY, carrier_name:CHARARRAY, tail_number:CHARARRAY);
flights = LOAD '/data/flights.csv' USING PigStorage(',') AS (day:INT, flight_number:CHARARRAY, tail_number:CHARARRAY, origin_airport_id:INT, dest_airport_id:INT, delay:INT, distance:INT);


movies2015 = FILTER movies BY year == 2015;

desiredgenres = FILTER moviegenres BY (genre == 'Comedy') OR (genre == 'Sci-Fi');

movies2015Genres = JOIN movies2015 BY movieid, desiredgenres BY movieid PARALLEL 4;

movies15Gtitles = FOREACH movies2015Genres GENERATE title AS title:CHARARRAY;

movies15GtitlesDIST = DISTINCT movies15Gtitles;

movies15gtitlesDISTORDERED = ORDER movies15GtitlesDIST BY title ASC;

dump movies15gtitlesDISTORDERED;

--condition
flightdelay600 = FILTER flights BY delay > 600;

--join
flightdelay600carriercodes = JOIN flightdelay600 BY tail_number, airplanes BY tail_number;

--project
flightcarriercodes = FOREACH flightdelay600carriercodes GENERATE carrier_code AS carrier_code:CHARARRAY;

--get distinct values
codeDistinct = DISTINCT flightcarriercodes;

codeDistinctOrdered = ORDER codeDistinct by carrier_code ASC;

--dump

dump codeDistinctOrdered;



flightdelay600carriercodes = JOIN flightdelay600 BY tail_number, airplanes BY tail_number PARALLEL 4;

--This script generates the airport code and city name of the airports in California(CA)
--The output is then ordered by the ascending order of the airport code.

--load the data from HDFS and define the schema
airports = LOAD '/data/airports.csv' USING PigStorage(',') AS (airport_id:INT, airport_code:CHARARRAY, city_name:CHARARRAY, state:CHARARRAY);

-- Find the airports in California(CA)
airportsInCA = FILTER airports BY state == 'CA';

-- Read the attributes we are interested in.
attributes = FOREACH airportsInCA GENERATE airport_code, city_name;

-- Order that by airport code.
orderAttributes= ORDER attributes BY airport_code;

-- output
DUMP orderAttributes;

movies = LOAD '/data/movies.csv' USING PigStorage(',') AS (movieid:INT, title:CHARARRAY, year:INT);

moviesperyear = GROUP movies BY year;

yearcountone = FOREACH moviesperyear GENERATE group AS yearone:INT , COUNT(movies) AS countone:int;

yearcounttwo = FOREACH moviesperyear GENERATE group AS yeartwo:INT , COUNT(movies) AS counttwo:int;

yearcomparison = JOIN yearcountone BY (yearone + 1), yearcounttwo BY yeartwo;

lessmoviesproduceduncut = FILTER yearcomparison BY countone > counttwo;

lessmoviesproduced = FOREACH lessmoviesproduceduncut GENERATE yeartwo AS year:INT , counttwo AS currentCount:INT , countone AS previousCount:INT;

STORE lessmoviesproduced INTO 'q4' USING PigStorage (',') ;
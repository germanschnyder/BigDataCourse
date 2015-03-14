REGISTER ../lib/elephant-bird-pig-4.6.jar;
REGISTER ../lib/slf4j-api-1.7.10.jar;

NYTArticles = LOAD 'Data/Article_1949*' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');



--
-- Keep only the 'speedtests'
--
Tests = FILTER @ BY (response.meta.time matches '40');


DUMP @;

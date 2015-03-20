REGISTER ../lib/elephant-bird-hadoop-compat-4.6.jar;
REGISTER ../lib/elephant-bird-core-4.6.jar;
REGISTER ../lib/elephant-bird-pig-4.6.jar;
REGISTER ../lib/slf4j-simple-1.7.10.jar;
REGISTER ../lib/slf4j-api-1.7.10.jar;

/* Parse all files as json */
raw_data = LOAD '../Data/Article_*.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (json: map[]);
            
/* Since these are json files, flatten the documents array */
docs = FOREACH raw_data GENERATE FLATTEN($0#'response'#'docs') AS documents;

/* just keep articles with Uruguay on their headline */
uruguay_articles = FILTER docs BY ($0#'headline'#'main' MATCHES '.*URUGUAY.*');

/* foreach article, grab the id and the publishing date */
results = FOREACH uruguay_articles GENERATE
    $0#'_id' AS ArticleID,
    $0#'word_count' AS WordCount,
    $0#'headline'#'main' AS KeyWords,
    GetYear(ToDate((chararray)$0#'pub_date')) AS Year;


rmf outputPig;

/* save results in hadoop filesystem */
STORE results INTO 'outputPig' USING JsonStorage();

/*DUMP results;*/

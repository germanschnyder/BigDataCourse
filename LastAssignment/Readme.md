
**Instructions:**


Requirements: Pig and Hadoop present and running (e.g. Hortonworks VM 2.2)

Move to src/ directory and run these steps:

1.  Run getarticles.sh to get all articles that mention Uruguay on New York Times database:

        sh 1.getarticles.sh <nyt-developer-articles-api-key>

2.  Run cleanarticles.pig script to clean metadata and dump the information that we want:

        pig -x local 2.cleanarticles.pig

3.  Run Hadoop MR to process all files and obtain final results:

        sh 3.get_results.sh


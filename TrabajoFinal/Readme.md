
**Instructions:**

1.  Run getarticles.sh to get all articles that mention Uruguay on New York Times database

        sh getarticles.sh <nyt-developer-articles-api-key>

2.  Run cleanarticles.pig script to clean metadata and dump the information that we want.

        pig -x local cleanarticles.pig

3.  Upload resulting files from Pig into HFS

        //TODO

4.  Run Hadoop MR to process all files and obtain final results

        //TODO


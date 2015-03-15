#!/usr/bin/env bash

#if we get a really small article, its just a stub */
minimumsize=200

#check that we got an api-key for querying nyt developer api
if [ -z "$1" ]
  then
    echo "You need an Article Search API key for this"
    echo "Usage:sh getarticles.sh nyt-api-key"
    exit
fi

#consider active years until now
for i in {1885..2016}
do
    endloop=0

    #theres a limit on how many pages we can get per query
    for j in {1..100}
    do

        #we query only if we are still getting valid data
        if [ $endloop = 0 ]; then

            #set the filename with some sense
            file_name="../Data/Article_"$i"_"$j".json"

            #make the query take one year per time and query about Uruguay
            begin_date=$i"0101"
            end_date=$i"1231"
            search_term="uruguay"
            url="http://api.nytimes.com/svc/search/v2/articlesearch.json?q=$search_term&page=$j&sort=oldest&api-key=$1:1:70240518&begin_date=$begin_date&end_date=$end_date"

            curl  $url > $file_name

            #check file size
            file_size=$(wc -c "$file_name" | cut -f 1 -d '.')

            #and end this years loop if we are not getting valid data
            if [ $file_size -le $minimumsize ]; then
                endloop=1
                rm $file_name
            fi
        fi
    done
 done


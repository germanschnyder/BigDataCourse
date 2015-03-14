#!/usr/bin/env bash
minimumsize=200

for i in {1885..2016}
do
    endloop=0
    for j in {1..100}
    do

        if [ $endloop = 0 ]; then
            file_name="../Data/Article_"$i"_"$j".json"

            begin_date=$i"0101"
            end_date=$i"1231"
            url="http://api.nytimes.com/svc/search/v2/articlesearch.json?q=uruguay&page=$j&sort=oldest&api-key=819d675e6f284d1abdf45623b28440ba:1:70240518&begin_date=$begin_date&end_date=$end_date"

            curl  $url > $file_name

            file_size=$(wc -c "$file_name" | cut -f 1 -d '.')

            if [ $file_size -le $minimumsize ]; then
                endloop=1
                rm $file_name
            fi
        fi
    done
 done


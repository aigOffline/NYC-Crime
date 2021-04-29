supported_year=( 2016 2017 2018 2019 )

if [ "$1" == 2016 ] || [ "$1" == 2017 ] || [ "$1" == 2018 ] || [ "$1" == 2019 ]
then
    spark-submit nypd-historic-extract.py /user/dt2259/nypd_arrests_historic.csv $1
    /usr/bin/hadoop fs -getmerge nypd_arrests_data_$1 ../sub-dataset/nypd_arrests_data_$1.csv 
    /usr/bin/hadoop fs -rm -r nypd_arrests_data_$1
elif [ "$1" = "all" ]
then
    for i in "${supported_year[@]}"
    do
        echo $i
        spark-submit nypd-historic-extract.py /user/dt2259/nypd_arrests_historic.csv $i
        /usr/bin/hadoop fs -getmerge nypd_arrests_data_$i ../sub-dataset/nypd_arrests_data_$i.csv 
        /usr/bin/hadoop fs -rm -r nypd_arrests_data_$i
    done
fi

rm -r ../sub-dataset/.*

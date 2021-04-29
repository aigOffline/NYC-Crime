from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: <file1> <year>", file=sys.stderr)
        exit(-1)

    year = sys.argv[2]

    spark = SparkSession \
        .builder \
        .appName("NYPD Arrest Historic Data Extract " + year) \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1]).createOrReplaceTempView("crime")

    crime_2016 = spark.sql("SELECT * FROM crime WHERE year(to_date(ARREST_DATE, 'mm/dd/yyyy')) = " + year)

    crime_2016.write.csv('nypd_arrests_data_' + year, header=True)
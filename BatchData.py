from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType

#Setting up PySpark
spark = SparkSession.builder \
          .appName('Batch Analytics') \
          .config('config.option', 'value') \
          .getOrCreate()

covidData = spark.read.csv(path='covid19.csv', header=True, inferSchema=True)

#Outputting data in CSV file
covidData.show()

#Printing off the schema for the CSV file
covidData.printSchema()

#Showing data after filering
filteredData = covidData.filter(covidData['continent'].isNotNull() & covidData['location'].isNotNull() \
 & covidData['date'].isNotNull() & covidData['total_cases'].isNotNull() & covidData['new_cases'].isNotNull() \
 & covidData['total_deaths'].isNotNull() & covidData['new_deaths'].isNotNull())
filteredData.show()

#Grouping by the location column, colleting identical data into groups
#Reducing duplicates and showing max number deaths in that location
filteredData.groupBy(['location']) \
.agg(f.max('total_deaths').alias('total_deaths')).show()

#Grouping by location, showing min number of cases in that location
filteredData.groupBy(['location']) \
.agg(f.min('total_cases').alias('min_cases')).show()

#Grouping by location, showing max number of cases in that location
filteredData.groupBy(['location']) \
.agg(f.max('total_cases').alias('max_cases')).show()

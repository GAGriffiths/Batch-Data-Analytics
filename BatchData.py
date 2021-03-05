#Task 1 and Task 2

#importing necessary libraries to run code
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType

#Setting up PySpark
spark = SparkSession.builder \
          .appName('Assignment1') \
          .config('config.option', 'value') \
          .getOrCreate()

#Task 2a
#Loading in COVID-19 CSV file
covidData = spark.read.csv(path='covid19.csv', header=True, inferSchema=True)

#Task 2b
#Outputting data in CSV file
covidData.show()

#Printing off the schema for the CSV file
covidData.printSchema()

#Task 2c
#Showing data before filtering
covidData.show()

#Showing data after filering
filteredData = covidData.filter(covidData['continent'].isNotNull() & covidData['location'].isNotNull() \
 & covidData['date'].isNotNull() & covidData['total_cases'].isNotNull() & covidData['new_cases'].isNotNull() \
 & covidData['total_deaths'].isNotNull() & covidData['new_deaths'].isNotNull())
filteredData.show()

#Task 3
#Grouping by the location column, colleting identical data into groups
#Reducing duplicates and showing max number deaths in that location
filteredData.groupBy(['location']) \
.agg(f.max('total_deaths').alias('total_deaths')).show()

#Task 4
#Grouping by location, showing min number of cases in that location
filteredData.groupBy(['location']) \
.agg(f.min('total_cases').alias('min_cases')).show()

#Grouping by location, showing max number of cases in that location
filteredData.groupBy(['location']) \
.agg(f.max('total_cases').alias('max_cases')).show()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import requests

# Initialize SparkSession
spark = SparkSession.builder.appName("EarthquakeDataProcessing").getOrCreate()

# Define the schema for the earthquakes DataFrame
schema = StructType([
    StructField("id", StringType(), True),
    StructField("latS", DoubleType(), True),
    StructField("lonE", DoubleType(), True),
    StructField("timestamp", IntegerType(), True),
    StructField("magnitude", DoubleType(), True),
    StructField("magnitude_type", StringType(), True),
    StructField("alert", StringType(), True),
    StructField("felt", IntegerType(), True)
])

# Create an empty DataFrame with the defined schema
earthquakes_df = spark.createDataFrame([], schema)

# Iterate over the years
for year in range(2019, 2021):
    url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={year}-01-01&endtime={year + 1}-01-01&minmagnitude=3"
    response = requests.get(url)
    earthquake_data = []

    for earthquake in response.json()['features']:
        if earthquake['properties']['type'] == 'earthquake':
            timestamp = earthquake['properties']['time'] // 1000 if len(str(earthquake['properties']['time'])) > 10 else earthquake['properties']['time']
            earthquake_data.append((
                earthquake['id'],
                earthquake['geometry']['coordinates'][1],
                earthquake['geometry']['coordinates'][0],
                timestamp,
                earthquake['properties']['mag'],
                earthquake['properties']['magType'],
                earthquake['properties']['alert'],
                earthquake['properties']['felt']
            ))

    # Create a DataFrame from the earthquake data and union it with the existing earthquakes DataFrame
    new_earthquakes_df = spark.createDataFrame(earthquake_data, schema)
    earthquakes_df = earthquakes_df.union(new_earthquakes_df)

# Save the DataFrame to a temporary table in Spark SQL
earthquakes_df.createOrReplaceTempView("earthquakes")

# You can perform Spark SQL queries on the "earthquakes" table
spark.sql("SELECT * FROM earthquakes WHERE magnitude >= 4.0").show()

# Stop the SparkSession
spark.stop()

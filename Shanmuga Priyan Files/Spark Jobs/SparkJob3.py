
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("TopRatedRestaurants").getOrCreate()

# Read the JSON file into a DataFrame
file_path = "gs://lab_4_dts/Project/yelp_academic_dataset_business.json"
yelp_business = spark.read.json(file_path)

# Filter businesses with 5-star ratings
top_rated_restaurants = yelp_business.filter(col("stars") == 5)

# Sort the filtered businesses in descending order based on review_count
sorted_top_rated = top_rated_restaurants.orderBy(col("review_count").desc())

# Select relevant columns and limit to top 5 businesses
result = sorted_top_rated.select("business_id", "name", "review_count").limit(5)

# Show the results
result.show()

# Stop the Spark session
spark.stop()

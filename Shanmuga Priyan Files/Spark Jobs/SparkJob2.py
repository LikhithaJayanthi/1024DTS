
''' This Program aims to calculates the average review count for businesses 
in each state, along with the highest average review count state-wise. 
It then shows the top 5 states with the highest average review counts.'''  


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Create a Spark session
spark = SparkSession.builder.appName("BusinessAnalysis").getOrCreate()

# Read the JSON file into a DataFrame
yelp_business = spark.read.json("gs://lab_4_dts/Project/yelp_academic_dataset_business.json")

# Filter out businesses with missing review_count values
filtered_businesses = yelp_business.filter(col("review_count").isNotNull())

# Calculate the average review count for businesses in each state
average_review_count_by_state = filtered_businesses.groupBy("state").agg(avg("review_count").alias("avg_review_count"))

# Find the state with the highest average review count
highest_avg_review_state = average_review_count_by_state.orderBy(col("avg_review_count").desc()).first()["state"]

# Show the state with the highest average review count
print("State with the highest average review count:", highest_avg_review_state)

# Show the top 5 states with the highest average review counts
top_states_avg_review = average_review_count_by_state.orderBy(col("avg_review_count").desc()).limit(5)
top_states_avg_review.show()

# Stop the Spark session
spark.stop()
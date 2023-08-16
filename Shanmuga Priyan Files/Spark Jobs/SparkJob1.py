'''This Program aims to calculates the average review count for businesses in each
 category and then displays the top 10 categories with the highest average review counts.'''



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, avg

# Create a Spark session
spark = SparkSession.builder.appName("CategoryAnalysis").getOrCreate()

# Read the JSON file into a DataFrame
yelp_business = spark.read.json("gs://lab_4_dts/Project/yelp_academic_dataset_business.json")

# Split the categories column into an array of categories
businesses_with_categories = yelp_business.withColumn("categories", split(col("categories"), ", "))

# Explode the categories array to get individual categories
businesses_with_categories = businesses_with_categories.withColumn("category", explode(col("categories")))

# Filter out businesses with missing review_count values
filtered_businesses = businesses_with_categories.filter(col("review_count").isNotNull())

# Calculate the average review count for each category
avg_review_count_by_category = filtered_businesses.groupBy("category").agg(avg("review_count").alias("avg_review_count"))

# Find the top 10 categories with the highest average review counts
top_categories = avg_review_count_by_category.orderBy(col("avg_review_count").desc()).limit(10)

# Show (top categories of businesses with the highest average review counts)
top_categories.show()

# Stop the Spark session
spark.stop()

#!/usr/bin/env python
# coding: utf-8

# # Spark Job 1 on Yelp Database

# In[53]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, desc, when, regexp_extract, size, corr
from pyspark.sql.types import StructType, StructField, StringType


# In[ ]:


# Create a Spark session
spark = SparkSession.builder \
    .appName("yelp_sparkjob") \
    .getOrCreate()

# Read yelp_checkin.csv
yelp_checkin_df = spark.read.csv("gs://dataproc-staging-us-central1-881550979572-mfizvoi4/yelp_spark_files/yelp_checkin.csv", header=True, inferSchema=True)

# Read yelp_business_merged.csv
yelp_business_merged_df = spark.read.csv("gs://dataproc-staging-us-central1-881550979572-mfizvoi4/yelp_spark_files/yelp_business_merged.csv", header=True, inferSchema=True)


# In[7]:


# Drop specified columns from yelp_checkin_df
columns_to_drop_checkin = ['latitude', 'longitude', 'address', 'postal_code', 'date']
yelp_checkin_df = yelp_checkin_df.drop(*columns_to_drop_checkin)


# In[12]:


# Filter businesses that are open at night

# Join the two DataFrames on the 'business_id' column
joined_df = yelp_business_merged_df.join(yelp_checkin_df, 'business_id')



# Finding out the average number of categories of yelp businesses

# In[18]:


# Calculate the average number of categories
average_categories = yelp_business_merged_df.select(avg("categories_count")).first()[0]

# Display the result
print("Average number of categories in the businesses:", average_categories)


# In[28]:


print("Query 1 : Is having more varied business gets them more reviews on yelp necessarily ?")


# In[54]:


# Filter rows with non-numeric values in categories_count column
yelp_business_merged_df_clean = yelp_business_merged_df.filter(col("categories_count").rlike("^[0-9]+$"))

# Calculate the average review count
average_review_count = yelp_business_merged_df_clean.select(avg("review_count")).first()[0]

# Find the top 20 businesses with the most categories
top_categories_businesses = yelp_business_merged_df_clean.orderBy(desc("categories_count")).limit(20)

# Determine if their review counts are greater than the average review count
top_categories_businesses_with_comparison = top_categories_businesses.withColumn(
    "review_count_gt_avg", when(col("review_count") > average_review_count, True).otherwise(False)
)

# Display the result with comparison
print("Top 20 businesses with the most categories and their review counts:")
top_categories_businesses_with_comparison.select("name", "categories_count", "review_count", "review_count_gt_avg").show()


# In[25]:


print("So it seems that having a lot of categories does not necessarily gain more reviews")


# In[29]:


print("Query 2 : Average review ratings for businesses with different review counts and businesses with more check-ins at night.")


# In[27]:


# Calculate the average review rating for businesses with high review counts
high_review_count_businesses = joined_df.filter(col("review_count") > 100)
average_rating_high_review_count = high_review_count_businesses.select(avg("stars")).first()[0]

# Calculate the average review rating for businesses with low review counts
low_review_count_businesses = joined_df.filter(col("review_count") <= 100)
average_rating_low_review_count = low_review_count_businesses.select(avg("stars")).first()[0]

# Find businesses with more check-ins at night
night_open_businesses = joined_df.filter(col("is_open") == 1).filter(col("Night Checkin") > 0)

# Calculate the average review rating for businesses with more night check-ins
average_rating_night_open_businesses = night_open_businesses.select(avg("stars")).first()[0]

# Display the insights with one decimal place
print("Average review rating for businesses with high review counts:", format(average_rating_high_review_count, '.1f'))
print("Average review rating for businesses with low review counts:", format(average_rating_low_review_count, '.1f'))
print("Average review rating for businesses with more night check-ins:", format(average_rating_night_open_businesses, '.1f'))


# In[40]:


yelp_user_df = spark.read.csv("gs://dataproc-staging-us-central1-881550979572-mfizvoi4/yelp_spark_files/yelp_user.csv", header=True, inferSchema=True)


# In[47]:


print("Query 3: Is there any signifant correlation betwen Elite Count and number of friends ?")


# In[46]:


# Calculate the correlation between elite_count and friends_count
correlation = yelp_user_df.select(corr("elite_count", "friends_count")).first()[0]

# Display the correlation with 2 digits after the decimal point
print("Correlation between elite_count and friends_count:", format(correlation, '.2f'))


# In[52]:


if correlation < 0.5:
    print("No significant effect on number of friends based on Elite count")
else :
    print ("Elite count has an effect on number of friends")


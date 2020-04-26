from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
import matplotlib as plt
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import io

spark = SparkSession \
        .builder \
        .appName("Twitter Data Analysis") \
        .getOrCreate()
df = spark.read.json("tweetdata.json")
df.createOrReplaceTempView("Virus_Sports")

sqlDF = spark.sql("SELECT user.location,count(text) as count FROM Virus_Sports WHERE place.country='United States' AND "
                  "user.location is not null GROUP BY user.location ORDER BY count DESC limit 10")

pd = sqlDF.toPandas()
pd.to_csv('output4.csv', index=False)

def plot4():
    pd.plot(kind="bar", x="location", y="count", title="tweets from states in US")

    bytes_image = io.BytesIO()
    # plt.savefig('foo.png')
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    return bytes_image

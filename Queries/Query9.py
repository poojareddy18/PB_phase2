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

sqlDF = spark.sql("SELECT user.name, retweeted_status.text AS "
                  "retweet_text,retweeted_status.retweet_count AS retweet_count"
                  " from Virus_Sports WHERE retweet_count IS NOT NULL ORDER BY "
                  "retweet_count DESC limit 20")

pd = sqlDF.toPandas()
pd.to_csv('output9.csv', index=False)

def plot9():
    pd.plot(kind="bar", x="retweet_text", y="retweet_count", title="Highest retweets ")

    bytes_image = io.BytesIO()
    # plt.savefig('foo.png')
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    return bytes_image

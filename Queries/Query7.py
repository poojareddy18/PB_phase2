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

sqlDF = spark.sql("SELECT user.name, user.followers_count, user.lang, text FROM"
                  " Virus_Sports WHERE text like '%virus%' "
                  "order by user.followers_count desc limit 10")

pd = sqlDF.toPandas()
pd.to_csv('output7.csv', index=False)

def plot7():
    pd.plot.pie(x="name", y="followers_count", title="tweets from states in US")

    bytes_image = io.BytesIO()
    # plt.savefig('foo.png')
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    return bytes_image

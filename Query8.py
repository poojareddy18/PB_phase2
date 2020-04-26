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

sqlDF = spark.sql("SELECT count(*) as count, user.name as name from Virus_Sports "
                  "where user.name is not null group by user.name order by count desc")

pd = sqlDF.toPandas()
pd.to_csv('output8.csv', index=False)


def plot8():
    pd.plot(kind="area", x="name", y="count", title="top 20 users with high tweets")

    bytes_image = io.BytesIO()
    # plt.savefig('foo.png')
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    return bytes_image

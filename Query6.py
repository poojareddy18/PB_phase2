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

sqlDF = spark.sql("SELECT user.verified,user.screen_name,user.followers_count FROM "
                  "Virus_Sports WHERE user.verified = false ORDER BY "
                  "user.followers_count DESC LIMIT 20")

pd = sqlDF.toPandas()
pd.to_csv('output6.csv', index=False)

def plot6():
    pd.plot(kind="barh", x="screen_name", y="followers_count", title="20 unverified user names")

    bytes_image = io.BytesIO()
    # plt.savefig('foo.png')
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    return bytes_image

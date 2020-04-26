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

sqlDF = spark.sql("SELECT substring(user.created_at,5,3) as month, count(user.id) as count from "
                  "Virus_Sports GROUP BY month")

pd = sqlDF.toPandas()
pd.to_csv('output1.csv', index=False)

def plot1():
    pd.plot(kind="area", x="month", y="count", title="number of user tweets")

    bytes_image = io.BytesIO()
    # plt.savefig('foo.png')
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    return bytes_image

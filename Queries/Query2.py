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


sqlDF = spark.sql("SELECT place.country,count(*) AS count FROM Virus_Sports WHERE place.country IS NOT NULL GROUP BY place.country "
                  "ORDER BY count DESC LIMIT 10")



pd = sqlDF.toPandas()
pd.to_csv('output2.csv', index=False)

def plot2():
    pd.plot(kind="bar", x="country", y="count", title="Tweets from different countries")

    bytes_image = io.BytesIO()
    # plt.savefig('foo.png')
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    return bytes_image

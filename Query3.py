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

sqlDF = spark.sql("SELECT substring(user.created_at,1,3) as day,count(*) as "
                  "count from Virus_Sports group by day")

pd = sqlDF.toPandas()
pd.to_csv('output3.csv', index=False)

def plot3():
    pd.plot.pie(y="count", title="week days", labels=['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'null'])

    bytes_image = io.BytesIO()
    # plt.savefig('foo.png')
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    return bytes_image

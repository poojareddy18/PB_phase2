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

sqlDF = spark.sql("SELECT substring(user.created_at,27,4) as year,count(*) "
                  "as Total from Virus_Sports where user.created_at is not null "
                  "group by substring(user.created_at,27,4) order by year desc")

pd = sqlDF.toPandas()
pd.to_csv('output11.csv', index=False)

def plot11():
    pd.plot(kind="barh", x="year", y="Total",
            title="Users created per Year")

    bytes_image = io.BytesIO()
    # plt.savefig('foo.png')
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    return bytes_image

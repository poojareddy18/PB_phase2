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

sqlDF = spark.sql("select count(*) as count,q.text from "
                  "(select case when text like '%Cricket%' then 'cricket' "
                  "when text like '%football%' then 'Football' when text like "
                  "'%Tennis%' then 'Tennis' when text like '%golf%' then 'Golf'"
                  " when text like '%baseball%' then 'Baseball'"
                  "WHEN text like '%Badminton%' THEN 'Badminton' WHEN text like '%Hockey%'"
                  " THEN 'Hockey' WHEN text like '%Volleyball%' THEN 'Volleyball'"
                  "when text like '%boxing%' then 'Boxing'when text like '%cycling%'"
                  " then 'Cycling'when text like '%swimming%' then 'Swimming'when "
                  "text like '%Archery%' then 'Archery'when text like '%Cricket%'"
                  " then 'cricket'when text like '%shooter%' then 'Shooter'when"
                  " text like '%bowling%' then 'Bowling'  end as text from Virus_Sports)q "
                  "where text <> 'null' group by q.text")

pd = sqlDF.toPandas()
pd.to_csv('output10.csv', index=False)

def plot10():
    pd.plot(kind="bar", x="text", y="count", title="tweets on sports")

    bytes_image = io.BytesIO()
    # plt.savefig('foo.png')
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    return bytes_image

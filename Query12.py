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

sqlDF = spark.sql("SELECT substring(user.screen_name,0,10) as User,max(user.followers_count) "
                  "as Followers FROM Virus_Sports WHERE text like '%sports%' group by "
                  "User order by Followers desc limit 5")

pd = sqlDF.toPandas()
pd.to_csv('output12.csv', index=False)

def plot12():
    pd.plot.area(x="User", y="Followers", title="Top5 most Followed Users related to sports Tweets")
    bytes_image = io.BytesIO()
    # plt.savefig('foo.png')
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    return bytes_image

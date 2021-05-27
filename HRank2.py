from pyspark.sql.functions import lit

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

if __name__ == "__main__":
    print("Session Started")

spark = SparkSession.builder.appName("HRank").master("local[*]") \
    .getOrCreate()

print("Session Created")

df = spark.read.text("G:/streamingfiles/TestFile.txt")
df1 = spark.read.text("G:/streamingfiles/TestFile1.txt")
df_ = df.select(f.concat(lit("1,"), f.col("Value")))
df1_ = df1.select(f.concat(lit("2,"), f.col("Value")))

print(df_.show())

final_df = df_.union(df1_)

print(final_df.show())

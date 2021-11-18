#imports
from pyspark import *
from pyspark.sql.functions import translate
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

#Load data from the csv file stored in the gcs bucket
df = spark.read.option("header", True).options(inferSchema='True', delimiter=',').csv(
    "gs://casestudy1_prudhvi/amazon_books_data.csv")

print(df.show(truncate=False))
#Print the schema of the dataset

print(df.printSchema())

#replacing the ',' in Name column with ';'
df = df.withColumn('Name', translate('Name', ',', ';'))
print(df.show(truncate=False))

#storing the cleansed data in bucket
df.write.csv('gs://casestudy1_prudhvi/cleansed_data')

#Average user ratings of Fiction,Non-Fiction and Overall books
fiction_rating = df.filter(df['Genre'] == 'Fiction').agg({'User Rating': 'avg'})
nonfiction_rating = df.filter(df['Genre'] == 'Non Fiction').agg({'User Rating': 'avg'})
total_rating = df.agg({'User Rating': 'avg'})

print(fiction_rating.show())
print(nonfiction_rating.show())
print(total_rating.show())


#storing the output in buckets
fiction_rating.write.csv('gs://casestudy1_prudhvi/avg_of_FictionBooks')
nonfiction_rating.write.csv('gs://casestudy1_prudhvi/avg_of_Non_Fiction_Books')
total_rating.write.csv('gs://casestudy1_prudhvi/avg_of_TotalBooks')

#sorting the books by alphabetical order and printing the last five books
df.orderBy(df.Name.desc()).limit(5).show(truncate=False)

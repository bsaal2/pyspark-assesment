from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, LongType, ShortType

spark = SparkSession.builder.appName('Region Info').master('local').getOrCreate()

# Custom schema
customSchema = StructType(
    [
    StructField("population", FloatType(), True),
    StructField("fertility", FloatType(), True),
    StructField("HIV", FloatType(), True),
    StructField("CO2", FloatType(), True),
    StructField("BMI_male", FloatType(), True),
    StructField("GDP", FloatType(), True),
    StructField("BMI_female", FloatType(), True),
    StructField("life", FloatType(), True),
    StructField("child_mortality", FloatType(), True),
    StructField("Region", StringType(), True),
    ]
  )

# Describe the structure of the dataframe
# df = spark.read.format('csv').option('header', 'true').load('./data/region_info.csv')
# df.printSchema()

# Load the dataframe using the custom defined schema
df = spark.read.format('csv').option('header', 'true').schema(customSchema).load('./data-src/region_info.csv')
df.printSchema()
df.show()

# Remove the region column
withoutAgeDF = df.drop(df.Region)
withoutAgeDF.show()

# Remove the missing/null values
filteredDF = withoutAgeDF.filter(
  df.fertility.isNotNull() & df.life.isNotNull()
)
filteredDF.show()

# Filter with the fertility and life 
filteredDF.filter("fertility > 1.0 and life > 70.0").show()
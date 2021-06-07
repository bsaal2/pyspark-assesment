from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

spark = SparkSession.builder.appName('Assesment').master('local').getOrCreate()

# Custom schema
customSchema = StructType(
    [
    StructField("id", IntegerType(), True),
    StructField("age", IntegerType(), True),
    StructField("BMI", FloatType(), True),
    StructField("PSA", FloatType(), True),
    StructField("TG", IntegerType(), True),
    StructField("Cholesterol", IntegerType(), True),
    StructField("LDLChole", IntegerType(), True),
    StructField("HDLChole", IntegerType(), True),
    StructField("Glucose", IntegerType(), True),
    StructField("Testosterone", FloatType(), True),
    StructField("BP_1", IntegerType(), True),
    ]
  )

# Describe the structure of the dataframe
# df = spark.read.format('csv').option('header', 'true').load('./data/medical_info.csv')
# df.printSchema()

# Load the dataframe using the custom defined schema
df = spark.read.format('csv').option('header', 'true').schema(customSchema).load('./data-src/medical_info.csv')
df.printSchema()
df.show()
    

# Remove the rows with null or missing values and create new dataframe
# Return the total count of the null/missing values
df2 = df.filter(
    df.id.isNull() | df.age.isNull() | df.BMI.isNull() | df.PSA.isNull() | df.TG.isNull() | df.Cholesterol.isNull()
    | df.LDLChole.isNull() | df.HDLChole.isNull() | df.Glucose.isNull() | df.Testosterone.isNull() |
    df.BP_1.isNull()
)
print("Total count with missing values: {0}".format(str(df2.count())))

# New dataframe by removing the null/missing values
df3 = df.filter(
  df.id.isNotNull() & df.age.isNotNull() & df.BMI.isNotNull() & df.PSA.isNotNull() & df.TG.isNotNull() & df.Cholesterol.isNotNull()
    & df.LDLChole.isNotNull() & df.HDLChole.isNotNull() & df.Glucose.isNotNull() & df.Testosterone.isNotNull() &
    df.BP_1.isNotNull()
)

# Calculations of min, max, median, standard deviation and variance of age
df3.select('age').summary('min', 'max', 'mean', '50%', 'stddev').show()

# Quartile info of the BMI
df3.select('BMI').summary('25%', '50%', '75%').show()

# Filter with age greater than 50 and BP_1 = 1
count = df3.filter((df.age > 50) & (df.BP_1 == 1)).count()
print('Filter count: {0}'.format(str(count)))
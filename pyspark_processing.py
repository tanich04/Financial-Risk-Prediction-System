
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName('FinancialRiskPrediction').getOrCreate()

# Load data from HDFS
data = spark.read.csv('hdfs://path/to/data.csv', header=True, inferSchema=True)

# Data preprocessing and feature engineering
data = data.withColumn('loan_income_ratio', col('loan_amnt') / col('annual_inc'))
data = data.dropna()

# Split the data
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

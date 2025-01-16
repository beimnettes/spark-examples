from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("StructuredStreamingExample").getOrCreate()

# Define schema for streaming data
schema = StructType().add("Name", StringType()).add("Age", IntegerType())

# Read streaming data from a directory
stream_df = spark.readStream.schema(schema).option("mode", "DROPMALFORMED").csv("/Users/beimnetfeleke/Desktop/asa1/input_stream_directory")
  # Replace with your input path

# Process data: Filter and display
filtered_stream = stream_df.filter("Age > 25")

# Output to the console
query = (
    filtered_stream.writeStream.outputMode("append")
    .format("console")
    .start()
)

query.awaitTermination()

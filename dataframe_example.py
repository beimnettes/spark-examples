from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("DataFrameExample").getOrCreate()

# Define file paths
input_file = "/Users/beimnetfeleke/Desktop/asa1/input_stream_directory/people.csv"
output_dir = "/Users/beimnetfeleke/Desktop/asa1/output/query_output"

try:
    # Read the CSV file into a DataFrame
    df = spark.read.csv(input_file, header=True, inferSchema=True)
    
    # Show the DataFrame
    print("Input DataFrame:")
    df.show()

    # Register the DataFrame as a temporary SQL view
    df.createOrReplaceTempView("people")

    # Run a SQL query to filter rows
    query_result = spark.sql("SELECT Name, Age FROM people WHERE Age > 25")
    
    # Show query results
    print("Filtered Results (Age > 25):")
    query_result.show()

    # Save the query results to the output directory
    query_result.write.csv(output_dir, mode="overwrite", header=True)
    print(f"Query results saved successfully in {output_dir}")
except Exception as e:
    print(f"Error: {e}")
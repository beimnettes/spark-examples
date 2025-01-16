from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext("local", "WordCount")

# Define file paths
input_file = "/Users/beimnetfeleke/Desktop/asa1/input_stream_directory/example.txt"
output_dir = "/Users/beimnetfeleke/Desktop/asa1/output/word_count_output"

try:
    # Read the text file into an RDD
    text_file = sc.textFile(input_file)
    
    # Perform word count
    word_counts = (
        text_file.flatMap(lambda line: line.split())  # Split lines into words
        .map(lambda word: (word.lower(), 1))  # Convert to lowercase and map words to (word, 1)
        .reduceByKey(lambda a, b: a + b)  # Reduce by summing counts
    )
    
    # Save the output or print results
    word_counts.saveAsTextFile(output_dir)
    print("Word count completed successfully.")
    print(word_counts.collect())  # Print results for debugging
except Exception as e:
    print(f"Error: {e}")
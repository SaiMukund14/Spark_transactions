from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import window

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("TransactionAlerts") \
    .getOrCreate()

# Initialize StreamingContext with a batch interval of 10 seconds
ssc = StreamingContext(spark.sparkContext, 10)

# Set the directory where transaction files are being written
transaction_dir = "stream/"

# Create a DStream that represents streaming data from the directory
lines = ssc.textFileStream(transaction_dir)

# Define a function to parse each transaction record
def parse_transaction(record):
    parts = record.split(',')
    # Ensure that there are at least 4 parts (transaction_time, account_number, transaction_type, amount)
    if len(parts) >= 4:
        transaction_type = parts[2]
        # Try to convert the amount to float, handle exceptions
        try:
            amount = float(parts[3])
            return (parts[1], transaction_type, amount)  # (account_number, transaction_type, amount)
        except ValueError:
            # If amount cannot be converted to float, return None
            return None
    else:
        return None

# Parse the lines of incoming transactions
transactions = lines.map(parse_transaction).filter(lambda x: x is not None)

# Define the three rules for generating alerts
def rule_one_filter(transaction):
    return transaction[2] > 8000

def rule_two_filter(account_count):
    return account_count > 3

def rule_three_filter(transaction):
    return transaction[2] < 8000 and transaction[1] == "Transfer"

# Define a function to write alerts to a file
def write_to_file(rdd, file_path):
    with open(file_path, 'a') as f:
        for alert in rdd.collect():
            f.write(str(alert) + '\n')

# Apply the rules and generate alerts
# Rule 1: Amount higher than 8000
rule_one_alerts = transactions.filter(rule_one_filter)
rule_one_alerts.foreachRDD(lambda rdd: write_to_file(rdd, transaction_dir + "rule_one_alerts.txt"))

# Rule 2: More than 3 transactions in the last 5 minutes for an account
windowed_counts = transactions.map(lambda x: (x[0], 1)).window(3600, 10).reduceByKey(lambda a, b: a + b)
rule_two_alerts = windowed_counts.filter(lambda x: rule_two_filter(x[1]))
rule_two_alerts.foreachRDD(lambda rdd: write_to_file(rdd, transaction_dir + "rule_two_alerts.txt"))

# Rule 3: Transactions less than 8000 of type Transfer
rule_three_alerts = transactions.filter(rule_three_filter)
rule_three_alerts.foreachRDD(lambda rdd: write_to_file(rdd, transaction_dir + "rule_three_alerts.txt"))

# Start the streaming context
ssc.start()
ssc.awaitTermination()
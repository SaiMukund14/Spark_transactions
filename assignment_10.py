import random
import datetime
import time
import os

# List of example transaction types
transaction_types = ['Deposit', 'Withdrawal', 'Transfer']

def generate_random_accounts(num_accounts):
    return [str(random.randint(100000000, 999999999)) for _ in range(num_accounts)]

def generate_transactions(accounts):
    transactions = []
    n = int(random.uniform(10,100))
    print("Generating "+str(n)+" transactions")
    for _ in range(n): # Generating n random transactions
        account_number = random.choice(accounts)
        transaction_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        transaction_type = random.choice(transaction_types)
        amount = round(random.uniform(10, 10000), 2) # Random amount between 10 and 10000
        transaction = f"{transaction_time},{account_number},{transaction_type},{amount}\n"
        transactions.append(transaction)

    wait_time = int(random.uniform(1,3))
    time.sleep(wait_time)
    return transactions

def write_transactions_to_file(transactions, filename):
    header = f"transaction_time,account_number,transaction_type,amount\n"
    with open(filename, 'w') as file:
        file.writelines(header)
        file.writelines(transactions)

# Create a directory if it doesn't exist
def create_directory_if_not_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

# Example usage
num_accounts = random.randint(1, 100) # Generate a random number of accounts
accounts = generate_random_accounts(num_accounts)

stream_dir = "stream/"
create_directory_if_not_exists(stream_dir)

cnt = 0
while True:
    cnt += 1
    transactions = []
    n = int(random.uniform(5,15))
    print("Generating "+str(n)+" sets of transactions")
    for _ in range(n):
        transactions.extend(generate_transactions(accounts))
    filename = f"{stream_dir}transactions{cnt}.csv"
    write_transactions_to_file(transactions, filename)
    print(f"Transactions have been written to {filename}.")

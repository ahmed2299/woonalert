import os
import schedule
import time
import subprocess
import json
from pymongo import MongoClient
from pymongo.errors import  PyMongoError

# Get the directory where the script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

def run_scrapy_script1():
    command1 = f'python {os.path.join(script_dir, "../spiders/pararius_spider.py")}'
    try:
        result = subprocess.run(command1, shell=True, check=True, capture_output=True)
        print("Pararius Script Output:", result.stdout.decode())
        upload_to_mongo('pararius_data.json', 'pararius_collection')
    except subprocess.CalledProcessError as e:
        print(f"Error running {command1}: {e}")

def run_scrapy_script2():
    command2 = f'python {os.path.join(script_dir, "../spiders/funda_spider.py")}'
    try:
        result = subprocess.run(command2, shell=True, check=True, capture_output=True)
        print("Funda Script Output:", result.stdout.decode())
        upload_to_mongo('funda_data.json', 'funda_collection')
    except subprocess.CalledProcessError as e:
        print(f"Error running {command2}: {e}")

def upload_to_mongo(json_filename, collection_name):
    try:
        # Connect to MongoDB
        mongo_uri = os.getenv('MONGO_URI', 'mongodb+srv://Gabriel:4wqUjZxSZ87Tcx0X@cluster0.nrvhn6m.mongodb.net/RealEstateDB?retryWrites=true&w=majority&appName=Cluster0')

        client = MongoClient(mongo_uri)
        db = client['RealEstateDB']
        collection = db[collection_name]

        # Load the JSON data
        with open(json_filename, 'r', encoding='utf-8') as file:
            data = json.load(file)

        # Clear the existing documents in the collection
        collection.delete_many({})

        # Insert the new data into MongoDB
        if isinstance(data, list):
            if data:  # Ensure the list is not empty
                collection.insert_many(data)
            else:
                print(f"No data to insert for {json_filename}")
        else:
            collection.insert_one(data)

        print(f"Uploaded {json_filename} to MongoDB collection {collection_name}")
    except (ConnectionError, PyMongoError) as e:
        print(f"Error connecting to MongoDB: {e}")
    except FileNotFoundError as e:
        print(f"File not found: {e}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON file: {e}")

# Schedule the first script to run at a specific time
schedule.every().day.at("00:00").do(run_scrapy_script1)

# Schedule the second script to run at a different time
schedule.every().day.at("00:00").do(run_scrapy_script2)  # Changed time for sequential execution

# Keep the script running to maintain the schedule
while True:
    schedule.run_pending()
    time.sleep(1)

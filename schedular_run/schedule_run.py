import subprocess
import json
import os
import logging
from pymongo import MongoClient
from http.server import SimpleHTTPRequestHandler, HTTPServer
import threading

# Configure logging
logging.basicConfig(filename='log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_scrapy_script1():
    try:
        logging.info("Starting pararius spider script")
        # Command to run your first Scrapy script
        command1 = 'python spiders/pararius_spider.py'
        result = subprocess.run(command1, shell=True, capture_output=True, text=True)
        logging.info(f"pararius spider script output: {result.stdout}")
        logging.error(f"pararius spider script error: {result.stderr}")
        # After running the script, upload the output JSON to MongoDB
        upload_to_mongo('pararius_data.json', 'pararius_collection')
        logging.info("Finished pararius spider script")
    except Exception as e:
        logging.error(f"Error running pararius script: {e}")

def run_scrapy_script2():
    try:
        logging.info("Starting funda spider script")
        # Command to run your second Scrapy script
        command2 = 'python spiders/funda_spider.py'
        result = subprocess.run(command2, shell=True, capture_output=True, text=True)
        logging.info(f"funda spider script output: {result.stdout}")
        logging.error(f"funda spider script error: {result.stderr}")
        # After running the script, upload the output JSON to MongoDB
        upload_to_mongo('funda_data.json', 'funda_collection')
        logging.info("Finished funda spider script")
    except Exception as e:
        logging.error(f"Error running funda script: {e}")

def upload_to_mongo(json_filename, collection_name):
    try:
        logging.info(f"Uploading {json_filename} to MongoDB collection {collection_name}")
        # Connect to MongoDB using the environment variable
        mongo_uri = os.getenv('MONGO_URI', 'mongodb+srv://Gabriel:4wqUjZxSZ87Tcx0X@cluster0.nrvhn6m.mongodb.net/RealEstateDB?retryWrites=true&w=majority&appName=Cluster0')
        logging.info(f"Mongo URI: {mongo_uri}")
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
                logging.warning(f"No data to insert for {json_filename}")
        else:
            collection.insert_one(data)

        logging.info(f"Uploaded {json_filename} to MongoDB collection {collection_name}")
    except Exception as e:
        logging.error(f"Error uploading to MongoDB: {e}")

def run_server():
    handler = SimpleHTTPRequestHandler
    httpd = HTTPServer(('0.0.0.0', 8000), handler)
    httpd.serve_forever()

if __name__ == "__main__":
    # Run the Scrapy scripts
    run_scrapy_script1()
    run_scrapy_script2()

    # Start the HTTP server
    run_server()

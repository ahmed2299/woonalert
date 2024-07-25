import schedule
import time
import subprocess
import json
import os
import logging
from pymongo import MongoClient
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading

# Configure logging
logging.basicConfig(filename='log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_scrapy_script1():
    try:
        logging.info("Starting pararius spider script")
        command1 = 'python spiders/pararius_spider.py'
        subprocess.run(command1, shell=True)
        upload_to_mongo('pararius_data.json', 'pararius_collection')
        logging.info("Finished pararius spider script")
    except Exception as e:
        logging.error(f"Error running pararius script: {e}")

def run_scrapy_script2():
    try:
        logging.info("Starting funda spider script")
        command2 = 'python spiders/funda_spider.py'
        subprocess.run(command2, shell=True)
        upload_to_mongo('funda_data.json', 'funda_collection')
        logging.info("Finished funda spider script")
    except Exception as e:
        logging.error(f"Error running funda script: {e}")

def upload_to_mongo(json_filename, collection_name):
    try:
        logging.info(f"Uploading {json_filename} to MongoDB collection {collection_name}")
        mongo_uri = os.getenv('MONGO_URI')
        client = MongoClient(mongo_uri)
        db = client['RealEstateDB']
        collection = db[collection_name]

        with open(json_filename, 'r', encoding='utf-8') as file:
            data = json.load(file)

        collection.delete_many({})

        if isinstance(data, list):
            if data:
                collection.insert_many(data)
            else:
                logging.warning(f"No data to insert for {json_filename}")
        else:
            collection.insert_one(data)

        logging.info(f"Uploaded {json_filename} to MongoDB collection {collection_name}")
    except Exception as e:
        logging.error(f"Error uploading to MongoDB: {e}")

schedule.every().day.at("01:42").do(run_scrapy_script1)
schedule.every().day.at("01:42").do(run_scrapy_script2)

class RequestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({'message': 'POST request received'}).encode())

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

def run_server():
    server_address = ('0.0.0.0', 8000)
    httpd = HTTPServer(server_address, RequestHandler)
    httpd.serve_forever()

if __name__ == "__main__":
    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.start()

    run_server()

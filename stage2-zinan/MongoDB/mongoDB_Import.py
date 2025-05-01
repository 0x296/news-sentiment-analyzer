import json
import os
from pymongo import MongoClient

# Load the JSON data from your file

# Get the current directory of the script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the file in the data folder
file_path = os.path.join(current_dir, '..', 'newsAPI', 'output.json')

with open(file_path, 'r') as file:
    data = json.load(file)


# Set your connection URI
atlas_uri = "mongodb+srv://group11:group11pass@group11-cluster.bxwy3jj.mongodb.net/"

# Connect to the MongoDB Atlas cluster
client = MongoClient(atlas_uri)

# Access your desired database and collection
db = client["News"]
collection = db["newsAPI"]


# Iterate through each article in the 'articles' list
for article in data['articles']:
 collection.insert_one(article)
 
# Close connection
client.close()

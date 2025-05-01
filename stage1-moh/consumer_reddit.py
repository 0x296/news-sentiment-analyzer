from kafka import KafkaConsumer
from json import loads
from pymongo import MongoClient

# Set your connection URI
atlas_uri = "mongodb+srv://group11:group11pass@group11-cluster.bxwy3jj.mongodb.net/"

# Connect to the MongoDB Atlas cluster
client = MongoClient(atlas_uri)

# Access your desired database and collection
db = client["News"]
collection = db["redditcomments"]



# Define Kafka broker address and topic
bootstrap_servers = ['129.114.27.101:30000']
topic_name = 'redditcomments'

# Create Kafka consumer
consumer = KafkaConsumer(topic_name,
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='reddit-group',
                         value_deserializer=lambda x: loads(x.decode('utf-8')))

# Consume and print messages
for message in consumer:
    comment = message.value
    collection.insert_one(comment)
    print(f"Inserted comment into MongoDB: {comment}")

 
# Close connection
client.close()


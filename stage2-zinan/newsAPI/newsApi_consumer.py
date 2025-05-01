from nltk.sentiment import SentimentIntensityAnalyzer
from kafka import KafkaConsumer
import json
from pymongo import MongoClient

import os

import nltk
nltk.download('vader_lexicon')

print("Consumer Start")
atlas_uri = os.getenv('ATLAS_URI', "mongodb+srv://group11:group11pass@group11-cluster.bxwy3jj.mongodb.net/")
db_name = os.getenv('DB_NAME', "News")
collection_name = os.getenv('COLLECTION_NAME',"news_articles")
topic_name = os.getenv('TOPIC_NAME','news_data_topic')
bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092')
bootstrap_servers = bootstrap_servers.split(',')

# Initialize NLTK sentiment analyzer
sid = SentimentIntensityAnalyzer()

# Set your MongoDB Atlas connection URI
#atlas_uri = "mongodb+srv://group11:group11pass@group11-cluster.bxwy3jj.mongodb.net/"

# Connect to the MongoDB Atlas cluster
client = MongoClient(atlas_uri)

# Access your desired database and collection
#db = client["News"]
db=client[db_name]
#collection = db["news_articles"]
collection = db[collection_name]

# Define Kafka broker address and topic
#bootstrap_servers = ['localhost:9092']
#topic_name = 'news_data_topic'

# Create Kafka consumer
consumer = KafkaConsumer(topic_name,
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='news-group',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x is not None else None)

# Function to calculate sentiment score
def get_sentiment_score(text):
    if text is not None:
        sentiment_scores = sid.polarity_scores(text)
        return sentiment_scores['compound']  # Using compound score as the overall sentiment
    else:
        return 0.0  # Return a default sentiment score if text is None

# Function to handle delete command from Kafka
def handle_delete_command(message):
    if "command" in message and message["command"] == "delete_collection":
        collection.delete_many({})
        print("MongoDB collection cleared.")

# Consume and insert messages into MongoDB with sentiment analysis
for message in consumer:
    message_value = message.value
    if message_value is not None:  # Check if message value is not None
        handle_delete_command(message_value)  # Check if it's a delete command
        if "command" not in message_value or message_value["command"] != "delete_collection":
            title_sentiment = get_sentiment_score(message_value.get('title', ''))
            description_sentiment = get_sentiment_score(message_value.get('description', ''))
            content_sentiment = get_sentiment_score(message_value.get('content', ''))

            # Calculate overall sentiment as the average of title, description, and content sentiments
            overall_sentiment = (title_sentiment + description_sentiment + content_sentiment) / 3.0

            # Add sentiment scores to the document before inserting into MongoDB
            message_value['title_sentiment'] = title_sentiment
            message_value['description_sentiment'] = description_sentiment
            message_value['content_sentiment'] = content_sentiment
            message_value['overall_sentiment'] = overall_sentiment

            collection.insert_one(message_value)
            print(f"Inserted article into MongoDB with sentiment analysis: {message_value}")

# Close connection
client.close()


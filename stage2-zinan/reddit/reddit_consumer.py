import json
import time
from datetime import datetime, timedelta
from nltk.sentiment import SentimentIntensityAnalyzer
from kafka import KafkaConsumer
from pymongo import MongoClient
import os

import nltk
nltk.download('vader_lexicon')

atlas_uri = os.getenv('ATLAS_URI', "mongodb+srv://group11:group11pass@group11-cluster.bxwy3jj.mongodb.net/")
db_name = os.getenv('DB_NAME', "News")
collection_name = os.getenv('COLLECTION_NAME',"reddit_comments")
average_sentiments_collection_name=os.getenv('AVERAGE_SENTIMENTS_COLLECTION_NAME',"average_sentiments")
topic_name = os.getenv('TOPIC_NAME','reddit_news')
bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092')
bootstrap_servers = bootstrap_servers.split(',')

# Initialize NLTK sentiment analyzer
sid = SentimentIntensityAnalyzer()

# Set your MongoDB Atlas connection URI
#atlas_uri = "mongodb+srv://group11:group11pass@group11-cluster.bxwy3jj.mongodb.net/"

# Connect to the MongoDB Atlas cluster
client = MongoClient(atlas_uri)

# Access your desired database and collection for Reddit comments
db = client[db_name]
collection = db[collection_name]

# Access the collection for average sentiment scores
average_sentiments_collection = db[average_sentiments_collection_name]

# Define Kafka broker address and topic
#bootstrap_servers = ['localhost:9092']
#topic_name = 'reddit_news'

# Create Kafka consumer
consumer = KafkaConsumer(topic_name,
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='reddit-group')

# Function to calculate sentiment score
def get_sentiment_score(text):
    if text is not None:
        sentiment_scores = sid.polarity_scores(text)
        return sentiment_scores['compound']  # Using compound score as the overall sentiment
    else:
        return 0.0  # Return a default sentiment score if text is None

# Function to calculate average sentiment score
def calculate_average_sentiment(sentiments):
    if sentiments:
        total_sentiment = sum(sentiments)
        average_sentiment = total_sentiment / len(sentiments)
        return average_sentiment
    else:
        return 0.0  # Return default if no sentiments are available

# Initialize variables for average sentiment calculation
average_window_duration = timedelta(seconds=10)
current_window_start = datetime.now()
sentiments_in_window = []

# Consume messages from Kafka and process Reddit comment data
for message in consumer:
    message_value = message.value.decode('utf-8')
    if message_value.startswith('{"command": "delete_all"}'):
        # Handle delete command
        print("Received delete command. Deleting data from MongoDB collections...")
        collection.delete_many({})  # Delete all documents from reddit_comments collection
        average_sentiments_collection.delete_many({})  # Delete all documents from average_sentiments collection
        print("Deletion completed.")
        continue  # Skip processing further if it was a delete command

    comment_json = json.loads(message_value)
    
    # Extract data from comment
    comment_id = comment_json['id']
    comment_name = comment_json['name']
    author_name = comment_json['author']
    comment_body = comment_json['body']
    subreddit_name = comment_json['subreddit']
    upvotes = comment_json['upvotes']
    downvotes = comment_json['downvotes']
    timestamp = comment_json['timestamp']
    permalink = comment_json['permalink']

    # Calculate sentiment score for the comment body
    comment_sentiment = get_sentiment_score(comment_body)

    # Prepare data for MongoDB insertion into Reddit comments collection
    comment_data = {
        'comment_id': comment_id,
        'name': comment_name,
        'author': author_name,
        'body': comment_body,
        'subreddit': subreddit_name,
        'upvotes': upvotes,
        'downvotes': downvotes,
        'timestamp': timestamp,
        'permalink': permalink,
        'comment_sentiment': comment_sentiment
    }

    # Insert comment data into MongoDB for Reddit comments
    collection.insert_one(comment_data)
    print(f"Inserted comment into MongoDB with sentiment analysis: {comment_data}")

    # Append sentiment score to list for average calculation
    sentiments_in_window.append(comment_sentiment)

    # Check if it's time to calculate and insert the average sentiment
    current_time = datetime.now()
    if current_time >= current_window_start + average_window_duration:
        # Calculate average sentiment
        average_sentiment_value = calculate_average_sentiment(sentiments_in_window)

        # Prepare data for MongoDB insertion into average sentiment collection
        average_sentiment_data = {
            'start_time': current_window_start,
            'end_time': current_time,
            'average_sentiment': average_sentiment_value
        }

        # Insert average sentiment data into MongoDB for average sentiments collection
        average_sentiments_collection.insert_one(average_sentiment_data)
        print(f"Inserted average sentiment into MongoDB: {average_sentiment_data}")

        # Reset variables for the next time window
        current_window_start = current_time
        sentiments_in_window = []

# Close connection
client.close()


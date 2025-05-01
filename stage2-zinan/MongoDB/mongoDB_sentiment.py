import json
from pymongo import MongoClient
import json
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Download the VADER lexicon for sentiment analysis if not already downloaded
nltk.download('vader_lexicon')

# Initialize the Sentiment Intensity Analyzer
sid = SentimentIntensityAnalyzer()

# Function to get sentiment score
def get_sentiment_score(text):
    return sid.polarity_scores(text)['compound']

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')

# Select database and collection
db = client['cc_test_db']
collection = db['test_collection']
collection_2 = db['test_collection_sentiment']

data = collection.find()

for document in data:
    title_sentiment = get_sentiment_score(document.get('title'))
    description_sentiment = get_sentiment_score(document.get('description'))
    content_sentiment = get_sentiment_score(document.get('content'))
    
    data = {
        'News Name' : document.get('source').get('name'),
    	'Title sentiment': title_sentiment,
    	'Description sentiment' : description_sentiment,
    	'Content sentiment' : content_sentiment
    }
    collection_2.insert_one(data)
    print(data)
    #print(document.get('source').get('name'))
    #print(document.get('title'))
    #print("Title Sentiment:", title_sentiment)
    #print("Description Sentiment:", description_sentiment)
    #print("Content Sentiment:", content_sentiment)
    print()

client.close()

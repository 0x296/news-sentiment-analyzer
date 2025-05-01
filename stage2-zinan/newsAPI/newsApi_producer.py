from kafka import KafkaProducer
import requests
import json
import os

# Kafka broker configuration
#bootstrap_servers = 'localhost:9092'
bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092')
bootstrap_servers = bootstrap_servers.split(',')
#topic = 'news_data_topic'  # Change the topic name as per your preference
topic_name = os.getenv('TOPIC_NAME','news_data_topic')
topic=topic_name

# Create Kafka Producer instance
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Function to handle delivery report from Kafka
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to topic '{msg.topic}' - partition: {msg.partition} - offset: {msg.offset}")

# Function to send delete command to MongoDB
def send_delete_command():
    command_message = {"command": "delete_collection"}
    producer.send(topic, json.dumps(command_message).encode('utf-8')).add_callback(delivery_report).get(timeout=10)
    print("Delete command sent to Kafka topic.")

# Fetch data from News API and stream JSON data to Kafka
def stream_news_data_to_kafka(search_query):
    send_delete_command()  # Send delete command before sending news data
    
    url = ('https://newsapi.org/v2/everything?'
           f'q={search_query}&'  # Use f-string to insert search query into the URL
           'from=2024-04-05&'
           'sortBy=popularity&'
           'apiKey=5edda667c3c44888ad5bff1276d0a023')

    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        articles = data.get('articles', [])
        
        for article in articles:
            message = json.dumps(article)
            producer.send(topic, message.encode('utf-8')).add_callback(delivery_report).get(timeout=10)

        print("Data streamed to Kafka topic successfully.")
    else:
        print("Error fetching data from News API:", response.status_code)

# Example usage
if __name__ == '__main__':
    search_query = input("Enter your search query: ")
    stream_news_data_to_kafka(search_query)


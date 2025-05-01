from json import dumps
from kafka import KafkaProducer
import configparser
import praw
import threading
import os

threads = []
#main_news_subreddits = ['worldnews', 'news', 'politics', 'economics', 'politics', 'sports']
main_news_subreddits = os.getenv('MAIN_NEWS_SUBREDDITS', 'worldnews,news,politics')
main_news_subreddits = main_news_subreddits.split(',')

topic_name = os.getenv('TOPIC_NAME','reddit_news')
kafka_topic = topic_name

bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092')
bootstrap_servers = bootstrap_servers.split(',')




class RedditProducer:
    def __init__(self, subreddit_list, cred_file: str = "credentials.cfg"):
        self.subreddit_list = subreddit_list
        self.reddit = self.get_reddit_client(cred_file)
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )
        self.filter_word = input("Enter a word to filter posts by: ")

    def get_reddit_client(self, cred_file) -> praw.Reddit:
        config = configparser.ConfigParser()
        config.read_file(open(cred_file))

        try:
            client_id = config.get("reddit", "client_id")
            client_secret = config.get("reddit", "client_secret")
            user_agent = config.get("reddit", "user_agent")
        except (configparser.NoSectionError, configparser.NoOptionError) as e:
            raise ValueError(f"Error reading config file: {e}")

        return praw.Reddit(
            user_agent=user_agent,
            client_id=client_id,
            client_secret=client_secret
        )

    def send_delete_command(self):
        delete_command = {"command": "delete_all"}
        self.producer.send(kafka_topic, value=delete_command)
        print("Delete command sent.")

    def fetch_top_posts(self, subreddit_name):
        subreddit = self.reddit.subreddit(subreddit_name)
        top_posts = subreddit.top("day", limit=10)
        return top_posts

    def extract_comments_from_top_posts(self, subreddit_name):
        top_posts = self.fetch_top_posts(subreddit_name)
        for post in top_posts:
            if self.filter_word.lower() in post.title.lower():
                post.comments.replace_more(limit=None)
                for comment in post.comments.list():
                    try:
                        comment_json = {
                            "id": comment.id,
                            "name": comment.name,
                            "author": comment.author.name,
                            "body": comment.body,
                            "subreddit": comment.subreddit.display_name,
                            "upvotes": comment.ups,
                            "downvotes": comment.downs,
                            "timestamp": comment.created_utc,
                            "permalink": comment.permalink,
                        }
                        self.producer.send(kafka_topic, value=comment_json)
                        print(f"Subreddit: {subreddit_name}, Comment: {comment_json}")
                    except Exception as e:
                        print("An error occurred:", str(e))

    def start_streaming_threads(self):
        for subreddit_name in self.subreddit_list:
            thread = threading.Thread(target=self.extract_comments_from_top_posts, args=(subreddit_name,))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()


if __name__ == "__main__":
    reddit_producer = RedditProducer(main_news_subreddits)
    reddit_producer.send_delete_command()
    reddit_producer.start_streaming_threads()


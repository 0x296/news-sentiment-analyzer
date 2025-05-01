from json import dumps
from kafka import KafkaProducer
import configparser
import praw
import threading

threads = []
main_news_subreddits = ['worldnews', 'news', 'politics', 'economics']
kafka_topic = 'reddit_news'


class RedditProducer:
    def __init__(self, subreddit_list: list[str], cred_file: str = "credentials.cfg"):
        self.subreddit_list = subreddit_list
        self.reddit = self.get_reddit_client(cred_file)
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )

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
        # Create a delete command JSON
        delete_command = {"command": "delete_all"}
        # Send the delete command to Kafka
        self.producer.send(kafka_topic, value=delete_command)
        print("Delete command sent.")

    def start_stream(self, subreddit_name) -> None:
        subreddit = self.reddit.subreddit(subreddit_name)
        for comment in subreddit.stream.comments(skip_existing=True):
            try:
                comment_json = {
                    "id": comment.id,
                    "name": comment.name,
                    "author": comment.author.name,
                    "body": comment.body,
                    "subreddit": comment.subreddit.display_name,
                    "upvotes": comment.ups,
                    "downvotes": comment.downs,
                    "over_18": comment.over_18,
                    "timestamp": comment.created_utc,
                    "permalink": comment.permalink,
                }

                self.producer.send(kafka_topic, value=comment_json)
                print(f"Subreddit: {subreddit_name}, Comment: {comment_json}")
            except Exception as e:
                print("An error occurred:", str(e))

    def start_streaming_threads(self):
        for subreddit_name in self.subreddit_list:
            thread = threading.Thread(target=self.start_stream, args=(subreddit_name,))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()


if __name__ == "__main__":
    reddit_producer = RedditProducer(main_news_subreddits)
    reddit_producer.send_delete_command()  # Send delete command before starting streaming
    reddit_producer.start_streaming_threads()


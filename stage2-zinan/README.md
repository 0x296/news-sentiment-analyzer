## Cloud Computing Project

# Overview of Project
This is a fake news sentiment analyser that uses data and news from multiple sources, pre-process them for sentiment analysis and detect the prevalence of fake news within these sources. Kafka will be used as the message broker to provide quick availability and scalability. A Kubernetes cluster will will be used for deployment with multiple docker containers being deployed. 

# Pipeline
We will use docker containers for each of the applications in each step of the pipeline, which are all publically in the docker hub registry. 

1. Data Ingestion:
   
   i) Reddit: We will use a containerized Python application that will connect to the reddit api using OAuth credentials. We will ingest the comments and posts, and convert them into a JSON format. After this pre-processing, we will send this data over to the Kafka broker. We will utilize the [Praw Library](https://praw.readthedocs.io/en/stable/) for interacting with the reddit API. To interact with the API, we need to create a reddit account and then create a script authorization that will allow us 100 requests per minute at [Reddit App Preferences](https://www.reddit.com/prefs/apps).

   

  

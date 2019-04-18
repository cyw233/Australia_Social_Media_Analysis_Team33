# Data Crawling

These three python files are used for collecting tweets from twitter.



## Usage

1. The `streaming.py` file will create a json file that contains all the data we crawled through the streaming API
2. The `thread_crawling.py` file is aimed to expand our usernames. To run this file, we need to use the json files created by `streaming.py`, it can find all the followers, and save to another text file.
3. The `rest.py` file is our final step of harvesting data, it will use the text files created by `thread_crawling.py`, it can find the history tweets in past 7 days based on the usernames of that text file.

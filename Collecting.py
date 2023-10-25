#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import json
import pandas as pd
import sys
from hdfs import InsecureClient
from bs4 import BeautifulSoup  # Import BeautifulSoup

# Function to scrape a Mastodon timeline
def scrape_mastodon_timeline(URL, params, since, hdfs_url, hdfs_filename):
    toots_data = []  # Store toots data
    is_end = False
    total_toots = 0

    try:
        while True:
            r = requests.get(URL, params=params)
            if r.status_code != 200:
                print(f"Failed to fetch data. Status code: {r.status_code}")
                break

            toots = json.loads(r.text)
            current_toots = len(toots)

            if current_toots == 0:
                print("No toots found within the date range")
                break

            total_toots += current_toots
            print(f'Scraped {str(total_toots)} toots...' , end='\r')

            for toot in toots:

                user = toot['account']

                # Convert timestamps to strings

                # Handle NaN values by replacing them with None
                in_reply_to_id = toot.get('in_reply_to_id', None)
                in_reply_to_account_id = toot.get('in_reply_to_account_id', None)
                # Handle other NaN fields similarly

                # Remove HTML tags from "content" and "note"
                content_text = BeautifulSoup(toot.get('content', ''), 'html.parser').get_text()
                note_text = BeautifulSoup(user.get('note', ''), 'html.parser').get_text()

                # Collect toots information
                toot_data = {
                    'id': toot['id'],
                    'created_at': toot.get('created_at', ''),
                    'in_reply_to_id': in_reply_to_id,
                    'in_reply_to_account_id': in_reply_to_account_id,
                    'sensitive': toot.get('sensitive', False),
                    'spoiler_text': toot.get('spoiler_text', ''),
                    'visibility': toot.get('visibility', ''),
                    'language': toot.get('language', ''),
                    'url': toot.get('url', ''),
                    'replies_count': toot.get('replies_count', 0),
                    'reblogs_count': toot.get('reblogs_count', 0),
                    'favourites_count': toot.get('favourites_count', 0),
                    'edited_at': toot.get('edited_at', ''),
                    'content': content_text,
                    'reblog': toot.get('reblog', None),
                    'account': {
                        'id': user['id'],
                        'username': user['username'],
                        'acct': user['acct'],
                        'display_name': user['display_name'],
                        'discoverable': user['discoverable'],
                        'group': user['group'],
                        'created_at': user['created_at'],
                        'note': note_text,
                        'url': user['url'],
                        'avatar': user['avatar'],
                        'avatar_static': user['avatar_static'],
                        'header': user['header'],
                        'followers_count': user['followers_count'],
                        'following_count': user['following_count'],
                        'statuses_count': user['statuses_count'],
                        'last_status_at': user['last_status_at'], 
                        'emojis': user['emojis'],
                        'fields': user['fields'],
                    },
                    'media_attachments': [attachment['url'] for attachment in toot.get('media_attachments', [])],
                    'mentions': [mention['acct'] for mention in toot.get('mentions', [])],
                    'tags': [tag['name'] for tag in toot.get('tags', [])],
                    'emojis': toot.get('emojis', []),
                    'card': toot.get('card', None),
                    'poll': toot.get('poll', None),
                }
                toots_data.append(toot_data)

            if is_end:
                break

            max_id = toots[-1]['id']
            params['max_id'] = max_id

    except KeyboardInterrupt:
        response = input("Do you want to continue scraping? (yes/no): ")
        if response.lower() == 'no':
            # Save scraped toots to HDFS before exiting
            print("Saving scraped toots...")
            save_to_json_hdfs(toots_data, hdfs_url, hdfs_filename)
            #print(f"Scrapped toots, total: {len(toots_data)} saved successfully")
            print(f'Number of posts retrieved: {str(len(toots_data))}', end='\r')
            sys.exit(0)

    return toots_data

# Function to save data to HDFS
def save_to_json_hdfs(data, hdfs_url, hdfs_filename):
    try:
        # Initialize an HDFS client
        hdfs_client = InsecureClient(hdfs_url, user='hadoop')

        # Create a new file with the current date in HDFS
        current_date = pd.Timestamp.now().strftime('%Y-%m-%d')
        hdfs_path = f'/Mostodon/Raw/{hdfs_filename}_{current_date}.json'

        # Check if the file already exists
        if hdfs_client.status(hdfs_path, strict=False):
            print(f"File {hdfs_path} already exists. Deleting it...")
            hdfs_client.delete(hdfs_path)

        # Process and format the data
        formatted_data = []
        
        for obj in data:
            formatted_obj = json.dumps(obj, separators=(',', ':'))
            formatted_data.append(formatted_obj)
            
        formatted_data_str = '\n'.join(formatted_data)

        # Save the formatted data to the HDFS file
        with hdfs_client.write(hdfs_path, encoding='utf-8') as writer:
            writer.write(formatted_data_str)

        print(f"Data saved to HDFS: {hdfs_path}")

    except Exception as e:
        print(f"Error saving data to HDFS: {str(e)}")

# Example usage:
if __name__ == "__main__":
    URL = 'https://mastodon.social/api/v1/timelines/public'
    params = {
        'limit': 40,
    }
    since = pd.Timestamp(year=2021, month=5, day=1, tz='utc')
    
    hdfs_url = 'http://172.26.139.82:9870'
    hdfs_filename = 'mastodon_dataaa'

    toots_data = scrape_mastodon_timeline(URL, params, since, hdfs_url, hdfs_filename)


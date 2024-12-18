import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import socket
import configparser
import json

config = configparser.ConfigParser()
config.read(f'docker/airflow/dags/reddit_api/moduls/reddit-cred.config')

class Prepare:
    def get_data_from_api(self,topic='dataengineering'):
        base_url = 'https://www.reddit.com/'
        auth = requests.auth.HTTPBasicAuth(
            config.get('REDDIT','user_id'),
            config.get('REDDIT','secret')
        )
        
        data = {"grant_type": "password", 
                    "username": config.get('REDDIT','username'),
                    "password": config.get('REDDIT','password')}

        r = requests.post(base_url + 'api/v1/access_token',
                            data=data,
                        headers={'user-agent': 'reddit-data'},
                auth=auth)

        token = 'bearer ' + r.json()['access_token']

        base_url = 'https://oauth.reddit.com'
        headers = {'Authorization': token, 'User-Agent': 'reddit-data'}

        payload = {'q': f'r/{topic}', 'limit': 100, 'sort': 'new'}
        response = requests.get(base_url + '/search', headers=headers, params=payload)
        values = response.json()
        return values

    def format_data(self,values):
        fetched_data = []
        for i in range(len(values['data']['children'])):
            data = dict()
            data['TS'] = values['data']['children'][i]['data']['created']
            data['TS_UTC'] = values['data']['children'][i]['data']['created_utc']
            data['TITLE'] = values['data']['children'][i]['data']['title'].replace('\n',' ').replace('/r','')
            data['TEXT'] = values['data']['children'][i]['data']['selftext'].replace('\n',' ').replace('/r','')
            data['NSFW'] = values['data']['children'][i]['data']['over_18']
            data['VOTE_RATIO'] = float(values['data']['children'][i]['data']['upvote_ratio'])
            data['SCORE'] = float(values['data']['children'][i]['data']['score'])
            data['URL'] = values['data']['children'][i]['data']['url']
            data['USER_NAME'] = values['data']['children'][i]['data']['author']
            data["WLS"] = values['data']['children'][i]['data']['wls']
            
            data["SUBREDDIT"] = values['data']['children'][i]['data']['subreddit']
            data["SUBREDDIT_TYPE"] = values['data']['children'][i]['data']['subreddit_type']
            data["SUBREDDIT_SUBSCRIBER_COUNT"] = values['data']['children'][i]['data']['subreddit_subscribers']

            fetched_data.append(data)
        return fetched_data

    def produce_kafka(self,fetched_data,topic_name):

        conf = {'bootstrap.servers': '192.168.89.83:9092',
                'client.id': socket.gethostname()}

        producer = Producer(conf)

        # check topicname
        if topic_name not in producer.list_topics().topics:
            ac = AdminClient(conf)
            topic_list = []
            topic_list.append(NewTopic(topic=topic_name, num_partitions=1, replication_factor=1))
            ac.create_topics(new_topics=topic_list, validate_only=False)

        # create ack
        def acked(err, msg):
            if err is not None:
                print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
            else:
                print("Message produced: %s" % (str(msg)))

        for item in range(len(fetched_data)):
            producer.produce(topic_name, key="data", value="{}".format(json.dumps(fetched_data[item])), callback=acked)

        producer.flush()

    def main(self,name,**context):
        keyword_list = config.get('KEYWORD','keywords').split(',')

        if( name is not None):
            print(f"Used {name} topics")
            keyword_list = name

        for kw in keyword_list:
            print("Current Topic:", kw)
            data = self.get_data_from_api(topic=kw)
            if data is None:
                print("Fail fetc data")
                break
            formated_data = self.format_data(data)
            print("Produce Kafka...")
            self.produce_kafka(fetched_data=formated_data,
                            topic_name=kw)
            print("Process Succesful...")















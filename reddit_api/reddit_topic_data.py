import preapare
import configparser
config = configparser.ConfigParser()

config.read('reddit_api/reddit-cred.config')

pr = preapare.Prepare()

keyword_list = config.get('KEYWORD','keywords').split(',')


for kw in keyword_list:
    print("Current Topic:", kw)
    data = pr.get_data_from_api(topic=kw)
    if data is None:
        print("Fail fetc data")
        break
    formated_data = pr.format_data(data)
    print("Produce Kafka...")
    pr.produce_kafka(fetched_data=formated_data,
                     topic_name=kw)
    print("Process Succesful...")
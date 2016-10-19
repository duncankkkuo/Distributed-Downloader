import paramiko
import json
import traceback
from kafka import KafkaConsumer, KafkaProducer
import os

# Json
with open('consumer.json', 'r') as reader:
    json_get = json.loads(reader.read())

# Connect to kafka
broker = json_get['kafka_server']['broker']
group = json_get['kafka_server']['group']
consumer = KafkaConsumer(group_id = group,
                         bootstrap_servers = broker)

# Add all topics to topic group
topic_all = []
for topic in json_get['kafka_server']['topic']:
    topic_id = json_get['kafka_server']['topic'][topic]
    topic_all.append(topic_id)

consumer.subscribe(topic_all)

# Get the json from kafka
for message in consumer:
    kafka_json_get = json.loads(message.value)

    # Json message get
    if(kafka_json_get != None):
        # Open transport
        host = kafka_json_get['observe_target']['host']
        port = kafka_json_get['observe_target']['port']
        transport = paramiko.Transport((host, port))

        # Auth
        username = kafka_json_get['observe_target']['username']
        password = kafka_json_get['observe_target']['password']
        protocol = kafka_json_get['observe_target']['protocol']
        file_dir_path = kafka_json_get['observe_target']['file_dir_path']
        finish_dir_path = kafka_json_get['observe_target']['finish_dir_path']
        local_dir_path = kafka_json_get['observe_target']['local_dir_path']
        file_name = kafka_json_get['observe_target']['file_name']
        file_size = kafka_json_get['observe_target']['file_size']
        topic = kafka_json_get['observe_target']['topic']

        file_path = file_dir_path + file_name
        finish_path = finish_dir_path + file_name
        local_path = local_dir_path + file_name
        transport.connect(username=username, password=password)

        # Sftp to observe_target
        sftp = paramiko.SFTPClient.from_transport(transport)

        #Check free disk size
        statvfs = os.statvfs(local_dir_path)
        free_size = statvfs.f_frsize * statvfs.f_bfree

        # Download
        try:
            if(free_size * 0.9 > file_size):
                sftp.get(file_path, local_path)
            else:
                producer = KafkaProducer(bootstrap_servers=broker)
                producer.send(topic, json.dumps(kafka_json_get).encode('ascii'))

            print local_path, file_path

        except IOError as e:
            if e.errno == 13:
                print 'Permission denied'

        except (EOFError, KeyboardInterrupt):
            print 'Server interrupt'

        except:
            traceback.print_exc()

        # Move finished files to finish_path
        #sftp.rename(file_path, finish_path)

        # Close
        sftp.close()
        transport.close()
        print 'Done'

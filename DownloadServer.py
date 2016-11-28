import paramiko
import json
import traceback
from kafka import KafkaConsumer, KafkaProducer
import os
import sys
import ftplib
import socket
import pdb

def connect(protocol, port, host, username, password):
    if (protocol == 'sftp'):
        try:
            paramiko.util.log_to_file('/tmp/paramiko.log')
            transport = paramiko.Transport((host, port))
            transport.connect(username=username, password=password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            return sftp
        except:
            print("SFTP is unavailable,please check the host,username and password!")
            sys.exit(0)

    elif (protocol == 'ftp'):
        try:
            ftp = ftplib.FTP(host)
            ftp.login(username, password)
            return ftp
        except socket.error, socket.gaierror:
            print("FTP is unavailable,please check the host,username and password!")
            sys.exit(0)

def download_file(connect, protocol, file_dir_path, local_dir_path, file_name):
    if (protocol == 'sftp'):
        try:
            #file_path = file_dir_path + file_name
            #local_path = local_dir_path + file_name
            file_path = os.path.join(file_dir_path,file_name)
            local_path = os.path.join(local_dir_path,file_name)
            connect.get(file_path, local_path)
        except:
            traceback.print_exc()

    elif (protocol == 'ftp'):
        try:
            file_path = os.path.join(file_dir_path,file_name)
            local_file_name = os.path.join(local_dir_path, file_name)
            file = open(local_file_name, 'wb')
            connect.retrbinary('RETR '+ file_path, file.write)
        except ftplib.error_perm, resp:
            traceback.print_exc()

def move_file(connect, protocol, file_path, finish_path):
    if (protocol == 'sftp'):
        try:
            print("file_path=%s,finish_path=%s\n") % (file_path,finish_path)
            connect.rename(file_path, finish_path)
        except:
            traceback.print_exc()

    elif (protocol == 'ftp'):
        try:
            con.rename(file_path, finish_path)
        except ftplib.error_perm, resp:
            traceback.print_exc()

def disconnect(connect, protocol):
    if (protocol == 'sftp'):
        connect.close()

    elif (protocol == 'ftp'):
        connect.quit()

def main():
    # debug mode
    #pdb.set_trace()
    # Json
    with open('consumer.json', 'r') as reader:
        json_get = json.loads(reader.read())
    print 'Reading from consumer.json'

    # Connect to kafka
    broker = json_get['kafka_server']['broker']
    group = json_get['kafka_server']['group']
    consumer = KafkaConsumer(group_id = group,
                             bootstrap_servers = broker)
    print 'Connect to kafka successly'

    # Add all topics to topic group
    topic_all = []
    for topic in json_get['kafka_server']['topic']:
        topic_id = json_get['kafka_server']['topic'][topic]
        topic_all.append(topic_id)

    consumer.subscribe(topic_all)

    # Get the json from kafka
    for message in consumer:
        kafka_json_get = json.loads(message.value)
        print 'Get json message from kafka successly'

        # Json message get
        if(kafka_json_get != None):
            # Auth
            username = kafka_json_get['observe_target']['username']
            password = kafka_json_get['observe_target']['password']
            protocol = kafka_json_get['observe_target']['protocol']
            file_dir_path = kafka_json_get['observe_target']['file_dir_path']
            finish_dir_path = kafka_json_get['observe_target']['finish_dir_path']
            local_dir_path = kafka_json_get['observe_target']['local_dir_path']
            file_name = kafka_json_get['observe_target']['file_name']
            file_size = kafka_json_get['observe_target']['file_size']
            host = kafka_json_get['observe_target']['host']
            port = kafka_json_get['observe_target']['port']
            topic = kafka_json_get['observe_target']['topic']

            #file_path = file_dir_path + file_name
            #finish_path = finish_dir_path + file_name
            #local_path = local_dir_path + file_name
            file_path = os.path.join(file_dir_path,file_name)
            finish_path = os.path.join(finish_dir_path,file_name)
            local_path = os.path.join(local_dir_path,file_name)

            con = connect(protocol, port, host, username, password)

            #Check free disk size
            statvfs = os.statvfs(local_dir_path)
            free_size = statvfs.f_frsize * statvfs.f_bfree

            # Download
            if(free_size * 0.9 > file_size):
                download_file(con, protocol, file_dir_path, local_dir_path, file_name)
            else:
                producer = KafkaProducer(bootstrap_servers=broker)
                producer.send(topic, json.dumps(kafka_json_get).encode('ascii'))

            print file_name + ' donwnload successly'

            # Move finished files to finish_path
            # move_file(con, protocol, file_path, finish_path)

            # Close
            disconnect(con, protocol)
            print 'Done'

if __name__ == "__main__":
  main()

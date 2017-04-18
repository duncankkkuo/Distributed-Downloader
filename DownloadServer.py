import paramiko
import json
import traceback
from kafka import KafkaProducer
from pykafka import KafkaClient
from pykafka.common import OffsetType
import os
import sys
import ftplib
import socket
import time
import pdb
import logging

logging.basicConfig(level=logging.DEBUG,
                    filename='/var/log/multi-download/DownloadServer.log',
                    format='%(asctime)s - %(levelname)s - %(message)s'
                    )
logger = logging.getLogger('DownloadServerLogger')

def connect(protocol, port, host, username, password):
    if (protocol == 'sftp'):
        paramiko.util.log_to_file('/var/log/multi-download/paramiko.log')
        try:
            transport = paramiko.Transport((host, port))
            transport.connect(username=username, password=password)
            logger.debug('connect to the target server '+host+' successfully')
            sftp = paramiko.SFTPClient.from_transport(transport)
            return {'status':'success', 'message':sftp}
        except:
            logger.error("SFTP failed, host="+host+', username='+username+', traceback='+traceback.format_exc())
            return {'status':'failed', 'message':traceback.format_exc()}

    elif (protocol == 'ftp'):
        try:
            ftp = ftplib.FTP(host)
            ftp.login(username, password)
            logger.debug('connect to the target server '+host+' successfully')
            return {'status':'success', 'message':ftp}
        except :
            logger.error("FTP failed, host="+host+', username='+username+', traceback='+traceback.format_exc())
            return {'status':'failed', 'message':traceback.format_exc()}

def download_file(connect, protocol, file_dir_path, local_dir_path, file_name):
    if (protocol == 'sftp'):
        file_path = os.path.join(file_dir_path,file_name)
        local_path = os.path.join(local_dir_path,file_name)
        try:
            connect.get(file_path, local_path)
            logger.debug('downlaod the file '+file_name+' successfully')
            return {'status':'success'}
        except IOError, err:
            logger.error('download the file '+file_path+' fail, traceback='+traceback.format_exc())
            return {'status':'failed', 'message':traceback.format_exc(), 'error':err}

    elif (protocol == 'ftp'):
        file_path = os.path.join(file_dir_path,file_name)
        local_file_name = os.path.join(local_dir_path, file_name)
        try:
            file = open(local_file_name, 'wb')
            connect.retrbinary('RETR '+ file_path, file.write)
            logger.debug('downlaod the file '+file_name+' successfully')
            return {'status':'success'}
        except IOError, err:
            logger.error('download the file '+file_path+' fail, traceback='+traceback.format_exc())
            return {'status':'failed', 'message':traceback.format_exc(), 'error':err}

def move_file(connect, protocol, file_path, finish_path):
    if (protocol == 'sftp'):
        try:
            connect.rename(file_path, finish_path)
            logger.debug('move the file '+file_path+' successfully, and sleep 1 sec')
            return {'status':'success'}
        except:
            logger.error('move the file '+file_path+' fail, traceback='+traceback.format_exc())
            return {'status':'failed', 'message':traceback.format_exc()}

    elif (protocol == 'ftp'):
        try:
            con.rename(file_path, finish_path)
            logger.debug('move the file '+file_path+' successfully, and sleep 1 sec')
            return {'status':'success'}
        except:
            logger.error('move the file '+file_path+' fail, traceback='+traceback.format_exc())
            return {'status':'failed', 'message':traceback.format_exc()}

def disconnect(connect, protocol, host):
    if (protocol == 'sftp'):
        try:
            connect.close()
            logger.debug('disconnect from target server '+host+' successfully')
            return {'status':'success'}
        except:
            logger.error('disconnect from target server '+host+' failed')
            return {'status':'failed', 'message':traceback.format_exc()}

    elif (protocol == 'ftp'):
        try:
            connect.quit()
            logger.debug('disconnect from target server '+host+' successfully')
            return {'status':'success'}
        except:
            logger.error('disconnect from target server '+host+' failed')
            return {'status':'failed', 'message':traceback.format_exc()}

def main():
    # debug mode
    #pdb.set_trace()

    # Json
    with open('consumer.json', 'r') as reader:
        json_get = json.loads(reader.read())

    logger.debug('read from the consumer.json successfully')

    # Connect to kafka
    broker = json_get['kafka_server']['broker']
    zookeeper = json_get['kafka_server']['zookeeper']
    group = json_get['kafka_server']['group']
    logger.debug('waiting for connecting to kafka server '+broker)
    client = KafkaClient(hosts=broker)
    logger.debug('connect to the kafka server '+broker+' with group '+group+' successfully')

    # Add all topics to topic group
    topic_all = []
    for topic in json_get['kafka_server']['topic']:
        topic_id = json_get['kafka_server']['topic'][topic]
        topic_all.append(topic_id.encode('ascii'))

    topic = client.topics[topic_all[0]]
    logger.debug('set up the all kafka topics '+str(topic_all)+' successly')

    consumer = topic.get_balanced_consumer(
        zookeeper_connect = zookeeper,
        consumer_group = group.encode('ascii'),
        auto_offset_reset = OffsetType.LATEST,
        auto_commit_enable = True
    )


    # Get the json from kafka
    for message in consumer:
        kafka_json_get = json.loads(message.value)
        # Json message get
        if(kafka_json_get != None):
            # Auth
            username = kafka_json_get['observe_target']['username']
            password = kafka_json_get['observe_target']['password']
            protocol = kafka_json_get['observe_target']['protocol']
            file_dir_path = kafka_json_get['observe_target']['file_dir_path']
            finish_dir_path = kafka_json_get['observe_target']['finish_dir_path']
            local_dir_path = kafka_json_get['observe_target']['local_dir_path']
            local_dir_file_count_limit = kafka_json_get['observe_target']['local_dir_file_count_limit']
            file_name = kafka_json_get['observe_target']['file_name']
            file_size = kafka_json_get['observe_target']['file_size']
            host = kafka_json_get['observe_target']['host']
            port = kafka_json_get['observe_target']['port']
            topic = kafka_json_get['observe_target']['topic']

            logger.debug('get the message of target server '+host+' information from kafka topic successfully')

            file_path = os.path.join(file_dir_path,file_name)
            finish_path = os.path.join(finish_dir_path,file_name)
            local_path = os.path.join(local_dir_path,file_name)

            connect_result = connect(protocol, port, host, username, password)
            if connect_result['status'] != 'failed':
                con = connect_result['message']
                #Check free disk size
                statvfs = os.statvfs(local_dir_path)
                free_size = statvfs.f_frsize * statvfs.f_bfree
                logger.debug('free size of target server now='+str(free_size))

                #Check count of local file
                logger.debug('check the file count from local path '+local_dir_path+', and to determine whether it less than '+str(local_dir_file_count_limit))
                if len(os.walk(local_dir_path).next()[2]) < local_dir_file_count_limit:

                    # Download
                    logger.debug('check the file '+file_path+' size to determine whether it less than free size*0.90')
                    if(free_size * 0.9 > file_size):
                        logger.debug('download begin '+file_name)
                        download_file_result = download_file(con, protocol, file_dir_path, local_dir_path, file_name)
                        time.sleep(10)
                        if download_file_result['status'] != 'failed':
                            logger.debug('download finished '+file_name)
                            logger.debug(file_name+' download status success, begin to move to remote delete folder')
                            # Move finished files to finish_path
                            logger.debug('try to move '+file_name+'')
                            move_file_result = move_file(con, protocol, file_path, finish_path)
                            if move_file_result['status'] != 'failed':
                                logger.debug(file_name+' move successfully')
                                time.sleep(1)
                        else:
                            if download_file_result['error'].errno == 2:
                            	logger.warning('download failed, remove temporary file of '+file_name)
                                try:
                                    os.remove(os.path.join(local_dir_path,file_name))
                                    logger.debug('temporary file of '+file_name+' is deleted')
                                except:
                                    logger.debug('temporary file of '+file_name+' failed')
                                    logger.error('traceback:'+traceback.format_exc())
                            else:
                                client = KafkaClient(hosts=broker)
                                client_topic = client.topics[topic.encode('ascii')]
                                producer = client_topic.get_producer(use_rdkafka=False)
                                producer.produce(json.dumps(kafka_json_get).encode('ascii'))
                                producer.stop()
                                logger.warning('download failed, put '+file_name+' back to kafka, and sleep 5 secs')
                               	time.sleep(5)
                    else:
                        client = KafkaClient(hosts=broker)
                        client_topic = client.topics[topic.encode('ascii')]
                        producer = client_topic.get_producer(use_rdkafka=False)
                        producer.produce(json.dumps(kafka_json_get).encode('ascii'))
                        producer.stop()
                        logger.warning('free size*0.90 is less than file size, put '+file_name+' back to kafka, and sleep 5 secs')
                        time.sleep(5)
                else:
                    client = KafkaClient(hosts=broker)
                    client_topic = client.topics[topic.encode('ascii')]
                    producer = client_topic.get_producer(use_rdkafka=False)
                    producer.produce(json.dumps(kafka_json_get).encode('ascii'))
                    producer.stop()
                    logger.warning('local file count exceed '+str(local_dir_file_count_limit)+', put '+file_name+' back to kafka, and sleep 5 secs')
                    time.sleep(5)

                # Close
                disconnect(con, protocol, host)

if __name__ == "__main__":
  main()

from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base
import paramiko
from datetime import datetime
import json
from pykafka import KafkaClient
import sys
import os
import ftplib
import socket
import fnmatch
import datetime
import logging
import traceback

logger = logging.getLogger('ObserveServerLogger')
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler('/var/log/multi-download/ObserveServer.log')
fh.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(ch)

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

def get_file_list(connect, protocol, file_dir_path, host):
    if (protocol == 'sftp'):
        files = []
        try:
            files = connect.listdir(file_dir_path)
            logger.debug('get the file list from '+file_dir_path+' of target server '+host+' successfully')
            return {'status':'success', 'message':files}
        except:
            logger.error('get the file list from '+file_dir_path+' failed, traceback='+traceback.format_exc())
            return {'status':'failed', 'message':traceback.format_exc()}

    elif (protocol == 'ftp'):
        files = []
        try:
            for file_name in connect.nlst(file_dir_path):
                files.append(file_name.replace(file_dir_path, ""))
            logger.debug('get the file list from '+file_dir_path+' of target server '+host+' successfully')
            return {'status':'success', 'message':files}
        except:
            logger.error('get the file list from '+file_dir_path+' failed, traceback='+traceback.format_exc())
            return {'status':'failed', 'message':traceback.format_exc()}

def get_file_date(connect, protocol, file_path):
    if (protocol == 'sftp'):
        try:
            mtime = connect.stat(file_path).st_mtime
            file_time = datetime.fromtimestamp(mtime)
            logger.debug('get the file '+file_path+' datetime successfully')
            return {'status':'success', 'message':file_time}
        except:
            logger.error('get the file '+file_path+' datedime failed, tracebace='+traceback.format_exc())
            return {'status':'failed', 'message':traceback.format_exc()}

    elif (protocol == 'ftp'):
        try:
            file_time = connect.sendcmd('MDTM ' + file_path)
            logger.debug('get the file '+file_path+' datetime successfully')
            return {'status':'success', 'message':file_time}
        except:
            logger.error('get the file '+file_path+' datedime failed, tracebace='+traceback.format_exc())
            return {'status':'failed', 'message':traceback.format_exc()}

def get_file_size(connect, protocol, file_path):
    if (protocol == 'sftp'):
        try:
            file_size = connect.stat(file_path).st_size
            logger.debug('get the file '+file_path+' size successfully')
            return {'status':'success', 'message':file_size}
        except:
            logger.error('get the file '+file_path+' size failed, tracebace='+traceback.format_exc())
            return {'status':'failed', 'message':traceback.format_exc()}

    elif (protocol == 'ftp'):
        try:
            file_size = connect.size(file_path)
            logger.debug('get the file '+file_path+' size successfully')
            return {'status':'success', 'message':file_size}
        except:
            logger.error('get the file '+file_path+' size failed, tracebace='+traceback.format_exc())
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
    # Json
    with open('producer.json', 'r') as reader:
        json_get = json.loads(reader.read())

    logger.debug('read from the producer.json successfully')

    # Sqlalchemy
    for server in json_get['observe_target']:
        # Auth
        host = json_get['observe_target'][server]['host']
        port = json_get['observe_target'][server]['port']
        username = json_get['observe_target'][server]['username']
        password = json_get['observe_target'][server]['password']
        protocol = json_get['observe_target'][server]['protocol']
        file_dir_path = json_get['observe_target'][server]['file_dir_path']
        finish_dir_path = json_get['observe_target'][server]['finish_dir_path']
        local_dir_path = json_get['observe_target'][server]['local_dir_path']
        local_dir_file_count_limit = json_get['observe_target'][server]['local_dir_file_count_limit']
        topic = json_get['observe_target'][server]['topic']
        sqlite_db = json_get['sqlite_db']['db_name']
        file_pattern = json_get['observe_target'][server]['file_pattern']
        kafka_server = json_get['kafka_server']['broker']
        delete_timer = json_get['delete_timer']['timer']
        logger.debug('get the json information of target server %s successfully.', host)

        logger.debug('waiting for connecting to target server '+host)
        connect_result = connect(protocol, port, host, username, password)

	    client = KafkaClient(hosts=kafka_server)
        client_topic = client.topics[topic.encode('ascii')]
        producer = client_topic.get_producer(use_rdkafka=False)
	    logger.debug('connect to the kafka server '+kafka_server+' successfully')

        if connect_result['status'] != 'failed':
            con = connect_result['message']

            # Sqlalchemy
            engine = create_engine('sqlite:///./'+sqlite_db, echo=False)
            metadata = MetaData(engine)
            logger.debug('create the sqlite successfully')

            Base = declarative_base()

            class Server(Base):
                __tablename__ = server

                id = Column(Integer, primary_key=True)
                name = Column(String)
                time = Column(String)
                size = Column(Integer)

                def __init__(self, name, time, size):
                    self.name = name
                    self.time = time
                    self.size = size
            # Check table exist
            if not engine.dialect.has_table(engine, server):
                Base.metadata.create_all(engine)
                logger.debug('create the table '+server+' successfully')

            Session = sessionmaker()
            Session.configure(bind=engine)
            session = Session()

            # List file to sql
            file_list_result = get_file_list(con, protocol, file_dir_path, host)
            if file_list_result['status'] != 'failed':
                file_list = file_list_result['message']
                for file_name in file_list:
                    if fnmatch.fnmatch(file_name, file_pattern):
                        # file_path = file_dir_path + file_name
                        file_path = os.path.join(file_dir_path,file_name)
                        file_time = datetime.datetime.now()
                        file_size_result = get_file_size(con, protocol, file_path)

                        # Check filename exist in sql
                        if(session.query(Server).filter(Server.name == file_name).count() == 0):
                            if file_size_result['status'] != 'failed':
                                file_size = file_size_result['message']
                                consumer_json = {
                                    "observe_target": {
                                        "host": host,
                                        "username": username,
                                        "password": password,
                                        "protocol": protocol,
                                        "port": port,
                                        "file_dir_path": file_dir_path,
                                        "finish_dir_path": finish_dir_path,
                                        "local_dir_path": local_dir_path,
                                        "local_dir_file_count_limit": local_dir_file_count_limit,
                                        "file_name": file_name,
                                        "file_size": file_size,
                                        "topic": topic
                                    }
                                }

                                s = Server(file_name, file_time, file_size)
                                session.add(s)
                                session.commit()
                                logger.debug('add the file '+file_name+' from '+file_dir_path+' of target server '+host+' to sqlite successfully')

                                # Send message to kafka
                                producer.produce(json.dumps(consumer_json).encode('ascii'))
                                logger.debug('send the message of file '+file_name+' information to kafka successfully')

            # Close
	        producer.stop()
            disconnect(con, protocol, host)

            # Delete data from sqlite if file time over regular timer
            now_time = datetime.datetime.now()
            if(session.query(Server).filter(Server.name != '').count() > 0):
                logger.debug('check the file datetime from table '+server+' to decide whether to delete')
                for i in session.query(Server).all():
                    time = datetime.datetime.strptime(i.time, '%Y-%m-%d %H:%M:%S.%f')
                    if(now_time - time).seconds > delete_timer:
                        session.delete(i)
                        session.commit()
                        logger.debug('delete the data '+i.name+' from table '+server)

if __name__ == "__main__":
  main()

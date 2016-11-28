from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base
import paramiko
from datetime import datetime
import json
from kafka import KafkaConsumer, KafkaProducer
import sys
import os
import ftplib
import socket
import fnmatch
import datetime


def connect(protocol, port, host, username, password):
    if (protocol == 'sftp'):
        paramiko.util.log_to_file('/tmp/paramiko.log')
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        return sftp

    elif (protocol == 'ftp'):
        try:
            ftp = ftplib.FTP(host)
            ftp.login(username, password)
            return ftp
        except socket.error, socket.gaierror:
            print("FTP is unavailable,please check the host,username and password!")
            sys.exit(0)


def get_file_list(connect, protocol, file_dir_path):
    if (protocol == 'sftp'):
        files = []
        try:
            files = connect.listdir(file_dir_path)
            return files
        except:
            print "No files in this directory"

    elif (protocol == 'ftp'):
        files = []
        try:
            for file_name in connect.nlst(file_dir_path):
                files.append(file_name.replace(file_dir_path, ""))
            return files
        except ftplib.error_perm, resp:
            if str(resp) == "550 No files found":
                print "No files in this directory"


def get_file_date(connect, protocol, file_path):
    if (protocol == 'sftp'):
        try:
            mtime = connect.stat(file_path).st_mtime
            file_time = datetime.fromtimestamp(mtime)
            return file_time
        except:
            print "No files in this directory"

    elif (protocol == 'ftp'):
        try:
            file_time = connect.sendcmd('MDTM ' + file_path)
            return file_time
        except ftplib.error_perm, resp:
            if str(resp) == "550 No files found":
                print "No files in this directory"

def get_file_size(connect, protocol, file_path):
    if (protocol == 'sftp'):
        try:
            file_size = connect.stat(file_path).st_size
            return file_size
        except:
            print "No files in this directory"

    elif (protocol == 'ftp'):
        try:
            file_size = connect.size(file_path)
            return file_size
        except ftplib.error_perm, resp:
            if str(resp) == "550 No files found":
                print "No files in this directory"

def disconnect(connect, protocol):
    if (protocol == 'sftp'):
        connect.close()

    elif (protocol == 'ftp'):
        connect.quit()

def main():
    # Json
    with open('producer.json', 'r') as reader:
        json_get = json.loads(reader.read())

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
        topic = json_get['observe_target'][server]['topic']
        sqlite_db = json_get['sqlite_db']['db_name']
        file_pattern = json_get['observe_target'][server]['file_pattern']
        kafka_server = json_get['kafka_server']['broker']
        delete_timer = json_get['delete_timer']['timer']

        con = connect(protocol, port, host, username, password)

        # Sqlalchemy
        engine = create_engine('sqlite:///./'+sqlite_db, echo=True)
        metadata = MetaData(engine)

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

        Session = sessionmaker()
        Session.configure(bind=engine)
        session = Session()

        # List file to sql
        file_list = get_file_list(con, protocol, file_dir_path)
        for file_name in file_list:
            if fnmatch.fnmatch(file_name, file_pattern):
                # file_path = file_dir_path + file_name
                file_path = os.path.join(file_dir_path,file_name)
                file_time = datetime.datetime.now()
                file_size = get_file_size(con, protocol, file_path)

                # Check filename exist in sql
                if(session.query(Server).filter(Server.name == file_name).count() == 0):
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
                            "file_name": file_name,
                            "file_size": file_size,
                            "topic": topic
                        }
                    }

                    s = Server(file_name, file_time, file_size)
                    session.add(s)
                    session.commit()
                    print '===============add file ' + file_name + ' to db================'

                    # Send message to kafka
                    producer = KafkaProducer(bootstrap_servers=kafka_server)
                    producer.send(topic, json.dumps(consumer_json).encode('ascii'))

        # Close
        disconnect(con, protocol)

        # Delete data from sqlite if file time over regular timer
        now_time = datetime.datetime.now()
        if(session.query(Server).filter(Server.name != '').count() > 0):
            for i in session.query(Server).all():
                time = datetime.datetime.strptime(i.time, '%Y-%m-%d %H:%M:%S.%f')
                print '===========check file ' + i.name + ' time================'
                if(now_time - time).seconds > delete_timer:
                    session.delete(i)
                    session.commit()
                    print '============file ' + file_name + ' delete from db============'


        print('Done')

if __name__ == "__main__":
  main()

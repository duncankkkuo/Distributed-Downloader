from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base
import paramiko
from datetime import datetime
import json
from kafka import KafkaConsumer, KafkaProducer

paramiko.util.log_to_file('/tmp/paramiko.log')

# Json
with open('producer.json', 'r') as reader:
    json_get = json.loads(reader.read())

# Concatenate server string for port 9092
kafka_server = ''
for server in json_get['observe_target']:
    kafka_server += json_get['observe_target'][server]['host'] + ':9092,'

kafka_server = kafka_server[:len(kafka_server) - 1]

# Sqlalchemy
while(True):
    for server in json_get['observe_target']:
        # Open transport
        host = json_get['observe_target'][server]['host']
        port = json_get['observe_target'][server]['port']
        transport = paramiko.Transport((host, port))

        # Auth
        username = json_get['observe_target'][server]['username']
        password = json_get['observe_target'][server]['password']
        protocol = json_get['observe_target'][server]['protocol']
        file_dir_path = json_get['observe_target'][server]['file_dir_path']
        finish_dir_path = json_get['observe_target'][server]['finish_dir_path']
        local_dir_path = json_get['observe_target'][server]['local_dir_path']
        topic = json_get['observe_target'][server]['topic']
        sqlite_db = json_get['sqlite_db']['db_name']
        transport.connect(username=username, password=password)

        # Sftp to observe_target
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Sqlalchemy
        engine = create_engine('sqlite:///./'+sqlite_db, echo=True)
        metadata = MetaData(engine)

        Base = declarative_base()

        class Server(Base):
            __tablename__ = server

            id = Column(Integer, primary_key=True)
            name = Column(String)
            date = Column(String)
            size = Column(Integer)
            kafka = Column(Integer)
            sftp = Column(Integer)

            def __init__(self, name, date, size):
                self.name = name
                self.date = date
                self.size = size
        #Check table exist
        if not engine.dialect.has_table(engine, server):
            Base.metadata.create_all(engine)

        Session = sessionmaker()
        Session.configure(bind=engine)
        session = Session()


        # List file to sql
        print sftp.listdir(file_dir_path)
        for file_name in sftp.listdir(file_dir_path):
            file_path = file_dir_path + file_name
            mtime = sftp.stat(file_path).st_mtime
            file_date = datetime.fromtimestamp(mtime)
            file_size = sftp.stat(file_path).st_size

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

                s = Server(file_name, file_date, file_size)
                session.add(s)

                # Send message to kafka
                producer = KafkaProducer(bootstrap_servers=kafka_server)
                producer.send(topic, json.dumps(consumer_json).encode('ascii'))

                session.commit()

        # Close
        sftp.close()
        transport.close()
        print('Done')

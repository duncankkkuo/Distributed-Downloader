ObserveServer.py :Load the producer.jason to search the observe target, send the message aboout what files are new generation to kafka.

DownloadServer.py :Catch the mesage from kafka, and load the consumer.json to check brokers， then download the new generation files from observe target server.

Use Sftp 

create by DuncanKuo ,2016.10.18

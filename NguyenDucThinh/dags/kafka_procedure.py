import argparse
import csv
import json
import socket
from confluent_kafka import Producer

def acked(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {msg}: {err}")
    else:
        print(f"Message produced: {msg}")
def send_message(bootstrap_server, topic, csv_file):
    conf = {
        "bootstrap.servers": bootstrap_server,
        "client.id": socket.gethostname(),
        "message.timeout.ms": 60000, #thoi gian cho
        "retries": 5, # so lan thu lai
        "retry.backoff.ms": 1000, #thoi gian nghi giua cac lan thu lai
    }
    producer = Producer(conf)
    
    try:
        with open(csv_file, mode='r',newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                message_value = json.dumps({
                    "userID": row["userId"],
                    "movieId": row["movieId"],
                    "rating": row["rating"],
                    "timestamp": row["timestamp"],
                }, ensure_ascii=False).encode('utf-8')
                producer.produce(topic, value=message_value, callback=acked)
                
    except BufferError as e:
        print(f"Buffer error: {str(e)}")
    except Exception as e:
        print(f"Failed to deliver message: {str(e)}")
        
    producer.flush()
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple kafka producer script to send csv data to kafka.")
    parser.add_argument("--bootstrap-servers", help="Kafka broker as a comma-separated list", required=True)
    parser.add_argument("--topic",help="Topic to publish the message to", required=True)
    parser.add_argument("--csv-file",help="Path to the CSV file",required=True)
    
    args = parser.parse_args()
    send_message(args.bootstrap_servers, args.topic, args.csv_file)
    
# python kafka_procedure.py --bootstrap-servers localhost:19094 --topic nguyenducthinh_topic --csv-file C:\Users\dthin\Desktop\20121001_NguyenDucThinh\ratings_small.csv
import argparse
import json
import os
from confluent_kafka import Consumer, KafkaException, KafkaError
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

def kafka_bigquery(bootstrap_servers, topic, consumer_group):
    def create_table(client, dataset_id, table_id, schema):
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        try:
            table = client.get_table(table_ref) #api request
            print("Table {} already exists.".format(table_id))
            return table_ref
        except NotFound:
            print("Table {} is not found, creating it ...".format(table_id))
            table = bigquery.Table(table_ref, schema=schema)
            table = client.create_table(table) #api request
            print ("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
            return table_ref
    def get_message(bootstrap_server, topic, consumer_group):
        conf = {
            'bootstrap.servers': bootstrap_server,
            'group.id': consumer_group,
            'auto.offset.reset': 'earliest'
        }
        consumer = Consumer(conf)
        consume_loop(consumer, topic)
    
    def send_to_bigquery(client, table_ref, data):
        errors = client.insert_rows_json(table_ref, [data])  # API request
        if not errors:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))
    def consume_loop(consumer, topic):
        bigquery_client = bigquery.Client()
        dataset_id = 'demo'
        table_id = 'demo5'
        # mỗi lần chạy thì phải tạo bảng mới
        schema = define_schema()
        table_ref = create_table(bigquery_client, dataset_id, table_id, schema)
        
        consumer.subscribe([topic])
        try:
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Error: {msg.error()}")
                else:
                    message_data = json.loads(msg.value().decode('utf-8'))
                    send_to_bigquery(bigquery_client, table_ref, message_data)
        except KafkaException as e:
            print(f"Kafka error: {e}")
        finally:
            consumer.close()
    def define_schema():
        return [
            bigquery.SchemaField("userID", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("movieId", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("rating", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
        ]
    get_message(bootstrap_servers, topic, consumer_group)
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka consumer with bigquery integration")
    parser.add_argument("--bootstrap_servers", type=str, required=True, help="Kafka broker as a comma-separated list")
    parser.add_argument("--topic", type=str, required=True, help="Topic to subcribe to")
    parser.add_argument("--consumer_group", type=str, required=True)
    
    args = parser.parse_args()
    kafka_bigquery(args.bootstrap_servers, args.topic, args.consumer_group)
    

#python kafka_consumer.py --bootstrap_servers localhost:29094 --topic nguyenducthinh_topic --consumer_group nguyenducthinh_cgroup
#Ingest clickstream data from Kafka.

from kafka import KafkaProducer
import json

bootstrap_servers = 'localhost:9092'
topic = 'clickstream_topic'

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def capture_clickstream_event(user_id, timestamp, url, country, city, browser, operating_system, device):
    
    clickstream_event = {
        'user_id': user_id,
        'timestamp': timestamp,
        'url': url,
        'country': country,
        'city': city,
        'browser': browser,
        'operating_system': operating_system,
        'device': device
    }

    producer.send(topic, value=clickstream_event)
    producer.flush()

capture_clickstream_event('123', '2023-05-21 10:30:00', 'https://DataCo.com', 'US', 'New York', 'Chrome', 'Windows', 'Desktop')



#To store the ingested clickstream data


from cassandra.cluster import Cluster

cluster = Cluster(['localhost'])  
session = cluster.connect()

keyspace = 'clickstream_keyspace'
table = 'clickstream_table'

session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }}")
session.execute(f"USE {keyspace}")
session.execute(f"CREATE TABLE IF NOT EXISTS {table} (row_key UUID PRIMARY KEY, user_id TEXT, timestamp TIMESTAMP, url TEXT, country TEXT, city TEXT, browser TEXT, operating_system TEXT, device TEXT)")

def store_clickstream_data(row_key, user_id, timestamp, url, country, city, browser, operating_system, device):
    insert_query = f"INSERT INTO {table} (row_key, user_id, timestamp, url, country, city, browser, operating_system, device) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    session.execute(insert_query, (row_key, user_id, timestamp, url, country, city, browser, operating_system, device))

store_clickstream_data('123e4567-e89b-12d3-a456-426614174000', 'user123', '2023-05-21 10:30:00', 'https://DataCo.com', 'India', 'Banglore', 'Chrome', 'Windows', 'Desktop')



#Periodically process the stored clickstream data
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg

spark = SparkSession.builder.appName("ClickstreamAnalysis").getOrCreate()

clickstream_df = spark.read.format("your_data_store_format").option("your_option", "your_value").load()

aggregated_df = clickstream_df.groupBy("url", "country").agg(
    count("row_key").alias("clicks"),
    countDistinct("user_id").alias("unique_users"),
    avg("time_spent").alias("avg_time_spent")
)

aggregated_df.show()

aggregated_df.write.format("your_data_store_format").option("your_option", "your_value").save()

spark.stop()



#Index the processed data in Elasticsearch


from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

es_host = 'localhost'
es_port = 9200
es_index = 'clickstream_index'
es_doc_type = 'clickstream_doc'

es = Elasticsearch([{'host': es_host, 'port': es_port}])

def transform_to_documents(aggregated_data):
    documents = []
    for row in aggregated_data.collect():
        document = {
            '_index': es_index,
            '_type': es_doc_type,
            '_source': {
                'url': row['url'],
                'country': row['country'],
                'clicks': row['clicks'],
                'unique_users': row['unique_users'],
                'avg_time_spent': row['avg_time_spent']
            }
        }
        documents.append(document)
    return documents

processed_data = ...  # Processed data from previous step
documents = transform_to_documents(processed_data)

bulk(es, documents)

es.indices.refresh(index=es_index)

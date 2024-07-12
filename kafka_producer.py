
from kafka import KafkaProducer
import json

# Set up Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Example data to send
data = {'loan_amnt': 5000, 'annual_inc': 60000, 'dti': 15, 'loan_income_ratio': 0.0833}

# Send data to Kafka topic
producer.send('financial_data', value=data)
producer.flush()

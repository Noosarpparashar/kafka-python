import time
from kafka import KafkaProducer
from data_dummy import get_registered_user
import json
def json_serializer(data):
    return json.dumps(data).encode("utf-8")
producer = KafkaProducer(bootstrap_servers=["ec2-15.compute-1.amazonaws.com:9092","ec238.compute-1.amazonaws.com:9092","ec2-15.compute-1.amazonaws.com:9092"],
                         value_serializer=json_serializer)

if __name__ == '__main__':
    while True:
        registerd_user = get_registered_user()
        print(registerd_user)
        producer.send("third_topic",registerd_user)
        #time.sleep(3)
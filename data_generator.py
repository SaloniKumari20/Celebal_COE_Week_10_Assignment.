import time
import random
from azure.eventhub import EventHubProducerClient, EventData


connection_str = 'Your_Input_Event_Hub_Connection_String'
eventhub_name = 'Your_Event_Hub_Name'

def generate_random_data():
    producer = EventHubProducerClient.from_connection_string(conn_str=connection_str, eventhub_name=eventhub_name)
    
    while True:
        value = random.randint(50, 100)
        print(f"Generated value: {value}")
        event_data = EventData(str(value))
        with producer:
            producer.send_batch([event_data])
        time.sleep(2)

if __name__ == "__main__":
    generate_random_data()

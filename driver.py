import kafka
import json
import sys
import uuid
import threading
import time
import requests
import argparse
def parse_command_line_args():
    parser = argparse.ArgumentParser(description='Node Registration Script')
    parser.add_argument('--node-ip', required=True, help='Node IP address')
    parser.add_argument('--orchestrator-ip', required=True, help='Orchestrator IP address')
    parser.add_argument('--kafka-broker-ip', required=True, help='Kafka broker IP address')
    return parser.parse_args()
    
    
#app = Flask(__name__)
args = parse_command_line_args()
print(1)
kafka_producer = kafka.KafkaProducer(
    bootstrap_servers=args.kafka_broker_ip,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

node_id = str(uuid.uuid4())

data = {
    "node_id": node_id,
    "node_IP": args.kafka_broker_ip,
    "message_type": "DRIVER_NODE_REGISTER"
}

kafka_producer.send('register_topic', value=data)

target_server_url = "http://localhost:8080/"


def send_heartbeat():
	request_count = 0
	while True:
		info = {
            'node_id': node_id,
            "heartbeat": "YES",
            #"request_count": request_count
        }
		kafka_producer.send('heartbeat', value=info)
		time.sleep(3)
		#request_count += 1

def median_calculator(x):
	sorted_val = sorted(x)
	length =len(x)
	if length % 2 == 0:
		mid1 = sorted_val[length //2 -1]
		mid2 = sorted_val[length //2]
		median = (mid1 + mid2)/2
	else:
		median = sorted_val[length //2]
	return median 

def calculate_metrics(latencies):
    # Calculate mean, median, min, and max latencies
    mean_latency = sum(latencies) / len(latencies) if latencies else 0
    median_latency = median_calculator(latencies) if latencies else 0
    min_latency = min(latencies) if latencies else 0
    max_latency = max(latencies) if latencies else 0

    return {
        "mean_latency": mean_latency,
        "median_latency": median_latency,
        "min_latency": min_latency,
        "max_latency": max_latency
    }


def send_metrics(test_id, report_id, latencies):
    metrics = calculate_metrics(latencies)

    message = {
        "node_id": node_id,
        "test_id": test_id,
        "report_id": report_id,
        "metrics": metrics,
    }

    kafka_producer.send('metrics', value=message)


heartbeat_thread = threading.Thread(target=send_heartbeat)
heartbeat_thread.start()

consumer_trigger = kafka.KafkaConsumer('trigger_topic', bootstrap_servers=args.kafka_broker_ip)

for message in consumer_trigger:
	info = json.loads(message.value)
	if info["trigger"] == "YES":
		consumer_config = kafka.KafkaConsumer('test_config', bootstrap_servers=args.kafka_broker_ip)
		for message in consumer_config:
			info_conf = json.loads(message.value)
            
			print(info_conf)
            
			if info_conf['test_type'] == 'tsunami':
				print("Starting tsunami test...")
				print("Please wait...")
				request_interval = float(info_conf["test_message_delay"])
				request_count = info_conf["test_message_count"]
				latencies = []
				current_request_count=0
				start_time = time.time()
				while current_request_count <= int(request_count):
					try:
						request_start_time = time.time()
						response = requests.get(target_server_url)
						request_end_time = time.time()
						latency = (request_end_time - request_start_time) * 1000  # Convert to milliseconds
						latencies.append(latency)
						print(response.text)
						current_request_count+=1
						print(current_request_count)
						time.sleep(request_interval)
					except Exception as e:
						print(f'Error: {str(e)}')
				total_time = request_end_time - start_time
				test_id = info_conf["test_id"]
				report_id = str(uuid.uuid4())
				throughput = int(request_count)/total_time
				message = {'node_id':node_id,'throughput':throughput,'message_type':'LOAD_TEST_COMPLETED'}
				send_metrics(test_id, report_id, latencies)
				time.sleep(4)
				kafka_producer.send('completed',value = message)
				print("Load test completed")
			elif info_conf['test_type'] == 'avalanche':
				print("Starting avalanche test...")
				print("Please wait...")
				latencies = []
				request_count = info_conf["test_message_count"]
				current_req_count=0
				start_time = time.time()
				while current_req_count <= int(request_count):
					try:
						request_start_time = time.time()
						response = requests.get(target_server_url)
						request_end_time = time.time()
						latency = (request_end_time - request_start_time) * 1000  # Convert to milliseconds
						latencies.append(latency)
						print(response.text)
						current_req_count+=1
						print(current_req_count,end=" ")
					except Exception as e:
						print(f'Error: {str(e)}')
				total_time = request_end_time - start_time
				test_id = info_conf["test_id"]
				report_id = str(uuid.uuid4())
				throughput = int(request_count)/total_time
				message = {'node_id':node_id,'throughput':throughput,'message_type':'LOAD_TEST_COMPLETED'}
				send_metrics(test_id, report_id, latencies)
				time.sleep(4)
				kafka_producer.send('completed',value = message)
				print("Load test completed")

import flask
import sys
import kafka
import uuid
import json
import argparse
import threading
from time import sleep
import subprocess

app = flask.Flask(__name__)

producer = kafka.KafkaProducer(value_serializer=lambda x: json.dumps(x).encode('utf-8'))

def parse_command_line_args():
    parser = argparse.ArgumentParser(description='Node Registration Script')
   # parser.add_argument('--node-ip', required=True, help='Node IP address')
    parser.add_argument('--orchestrator-ip', required=True, help='Orchestrator IP address')
    parser.add_argument('--kafka-broker-ip', required=True, help='Kafka broker IP address')
    parser.add_argument('--num-of-dnodes',required=True, help='no. of driver nodes')
    return parser.parse_args()
    
args = parse_command_line_args()
num_driver_nodes = int(args.num_of_dnodes)
print(num_driver_nodes )

registered_nodes = {}
report_id = []
info_list = []
result = []
target_throughput = 0
#num_driver_nodes = int(sys.argv[1]) if len(sys.argv)>1 else 1

# def start_driver_nodes():
#         try:
#             print("running driver nodes")
#             subprocess.Popen(["python3", "driver.py"])
#         except Exception as e:
#             print(e)


def runtime_controller():
    consumer = kafka.KafkaConsumer('heartbeat', bootstrap_servers=args.kafka_broker_ip)

    for message in consumer:
        info = json.loads(message.value)
        node_id = info["node_id"]
        node_status = info["heartbeat"]
        registered_nodes[node_id] = node_status
        if len(registered_nodes) == num_driver_nodes:
            break
def calculate_median(values):
	sorted_val = sorted(values)
	length =len(values)
	if length % 2 == 0:
		mid1 = sorted_val[length //2 -1]
		mid2 = sorted_val[length //2]
		median = (mid1 + mid2)/2
	else:
		median = sorted_val[length //2]
	return median 

def metrics_controller():
    metrics_data = []
    global report_id
    report_id = []
    consumer_metrics = kafka.KafkaConsumer('metrics', bootstrap_servers=args.kafka_broker_ip)
    count=num_driver_nodes
    overall_mean_sum=0
    overall_median=[]
    overall_min=[]
    overall_max=[]    
    for message in consumer_metrics:
        info_metrics = json.loads(message.value)
        print("infometrics",info_metrics)
        count-=1
        overall_mean_sum += info_metrics["metrics"]["mean_latency"]
        overall_median.append(info_metrics["metrics"]["median_latency"])
        overall_min.append(info_metrics["metrics"]["min_latency"])
        overall_max.append(info_metrics["metrics"]["max_latency"])
        test_id = info_metrics["test_id"]
        report_id.append(info_metrics["report_id"])
        metrics_data.append(info_metrics["metrics"])
        print("metricsdata",metrics_data)
        if count==0:
        	total_median = calculate_median(overall_median)
        	total_metrics = {
        	"mean_latency": overall_mean_sum / len(overall_median),
        	"median_latency": total_median,
	        "min_latency": min(overall_min),
        	"max_latency": max(overall_max)
        	}
        	metrics_data.append(total_metrics)
        	return metrics_data
        #print("debug")
    return metrics_data

@app.route('/')
def load_html():
    # Render the HTML template named 'index.html' in the 'templates' folder
    return flask.render_template('index.html')

@app.route('/dashboard')
def dashboard():
    return flask.render_template('output.html', info_list=info_list, result=result,
                                 target_throughput=target_throughput, metrics_data=metrics_data)

@app.route('/load_test', methods=['POST'])
def load_test():
    x = flask.request.form
    #print(x)
    unique_id = str(uuid.uuid4())
    print(x.get("test_type"))
    data_conf = {
        "test_id": unique_id,
        "test_type": x.get("test_type"),
        "test_message_delay": x.get("test_message_delay"),
        "test_message_count": x.get("target_requests")
    }
    if data_conf["test_type"] == "avalanche":
        data_conf["test_message_delay"] = 0

    data = {
        "test_id": unique_id,
        "trigger": "YES",
    }

    # Start driver nodes in separate threads
    
    threads = []
    global registered_nodes
    registered_nodes = {}
    runtime_controller()
    print("Checking if all nodes are fine...")
    if all(node_status == "YES" for node_status in registered_nodes.values()):
        print("All nodes active. Starting load test...")
        
        	
        producer.send('trigger_topic', value=data)
        sleep(1)
        producer.send('test_config', value=data_conf)
        global info_list
        info_list = []
        computed_metrics = metrics_controller()
        count = num_driver_nodes
        sleep(3)
        consumer_completed = kafka.KafkaConsumer('completed', bootstrap_servers=args.kafka_broker_ip)
        #print("debug2")
        try:
            for message in consumer_completed:
                print("Load test completed")
                info_list.append(json.loads(message.value))
                count -= 1
                #print("debug1")
                if count == 0:
                    break
        except Exception as e:
            print(e)

        global result
        global target_throughput
        result = []
        for info in info_list:
            if info["throughput"] < int(x.get("target_throughput")):
                result.append("Target not achieved")
            else:
                result.append("Target throughput achieved")
        total_result = computed_metrics[-1]
        return flask.render_template('output.html', info_list=info_list, result=result,
                                     target_throughput=x.get("target_throughput"), metrics_data=computed_metrics, unique_id = unique_id, report_id = report_id, total_metrics = total_result)



if __name__ == '__main__':
    app.run(host=args.orchestrator_ip, port=5000)


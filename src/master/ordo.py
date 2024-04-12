import paho.mqtt.client as mqtt
import time
import random
import json

import math

import numpy as np


#Task = simple algorithm to be executed on distant machines
#Operation = instance of a task with input and result
#Program = N block of instruction that can request operation of a specific task on distant machine. each block is executed once the last one is complete.

#Worker = class that handle the lifecycle of a distant machine. periodically update the status
#Scheduler = algorithm that distribute the requested operation 


#Operation queue for current program block

#Logs of all task that where run, for which program, on which worker, the time it took 
operation_history = []

#all possible task 
task_list = [
	{
		"id": 0,
		"name": "sum_ab",
		"task": "r = data['a'] + data['b'];time.sleep(3)"
	},
	{
		"id": 1,
		"name": "mult_1d_a_b",
		"task": "for i in range(len(data['a'])): r += data['a'][i]*data['b'][i]"
	}
]

#The current operation queue (the next program block will be triggered when empty) 
operation_queue = []

#Program list
programs = []

#Program that compute the sum of 2 numbers (complexity: O(1)
def sumab_start(input_data):
	queue_operation(0, input_data)

def sumab_end(input_data):
	result = operation_history[len(program_history) - 1]["operations"][0]["result"]
	end_program(result)

program_sum = {
	"id" : 0,
	"name": "sum a b",
	"program_blocks": [sumab_start, sumab_end],
	"next_block": 0
}
programs.append(program_sum)

#Program that compute the matricial multiplication of 2 arbitrary large matrix (complexity: O(n^3))
def matmult(input_data):
	mat_a = input_data["a"]
	mat_b = np.transpose(np.array(input_data["b"])).tolist()

	for line_a in mat_a:
		for line_b in mat_b:
			operation_data = {
				"a": line_a,
				"b": line_b
			}
			queue_operation(1, operation_data)

def matmult_compute_final_result(input_data):
	n = len(input_data["a"])
	result = [[0 for x in range(n)] for y in range(n)]
	operations = operation_history[len(program_history) - 1]["operations"]

	for op in operations:
		k = op["operation_id"]
		i = math.floor(k / n)
		j = k % n
		result[i][j] = op["result"]
	end_program(result)

program_matmult = {
	"id" : 1,
	"name": "matrix multiplication",
	"program_blocks": [matmult, matmult_compute_final_result],
	"next_block": 0
}
programs.append(program_matmult)


#Dataset
exemple_data_for_sum = [
{
	"a": 3,
	"b": 4
},
{
	"a": 1,
	"b": 7
},
{
	"a": 25,
	"b": 30
},
{
	"a": 883,
	"b": 12
},
{
	"a": 34,
	"b": 91
}
]

program_history = []

current_program_type = -1
is_program_runnning = False
waiting_for_block_completion = False
#MQTT Topics:

#Subscriptions
system_response_topic = "pi4broker/system_response"
operation_result_topic = "pi4broker/operation_result"

#Publisher
request_system_topic = "pi4broker/request_system"


#dict of current Workers (dynamically updated whenever a worker is connected to the broker)
worker_list = {}

class Worker():
	def __init__(self, ip):
		self.ip = ip
		self.request_operation_topic = "pi4broker/request_operation_" + ip
		self.set_task_topic = "pi4broker/set_task_" + ip

		self.available = True
		#Init current program to -1, this will be changed whenever a first operation is sent to this worker
		self.current_task_id = -1

	def update_specs(self, cpu, ram, battery, current_task_id):
		self.cpu = cpu
		self.ram = ram
		self.battery = battery
		self.current_task_id = current_task_id
		self.score_worker()

	#TODO
	def score_worker(self):
		self.score = 0

	def send_task(self, task_id):
		self.current_task_id = task_id
		mqttc.publish(self.set_task_topic, payload=json.dumps(task_list[task_id]), qos=0, retain=False)

	def send_operation(self, operation_id, input_data):
		send_operation_payload = {}
		send_operation_payload["operation_id"] = operation_id
		send_operation_payload["input_data"] = input_data
		mqttc.publish(self.request_operation_topic, payload=json.dumps(send_operation_payload), qos=0, retain=False)

		new_operation_history_entry = {}
		new_operation_history_entry["operation_id"] = operation_id
		new_operation_history_entry["worker_ip"] = self.ip
		new_operation_history_entry["input_data"] = input_data
		new_operation_history_entry["start_time_sec"] = time.time()
		operation_history[len(program_history) - 1]["operations"].append(new_operation_history_entry)
		self.available = False


def add_or_update_workers(msg):
	ip = msg["ip"]
	if ip not in worker_list.keys():
		worker_list[ip] = Worker(ip)
		print("New worker connected! ip = " + ip)
	worker_list[ip].update_specs(msg["cpu"], msg["ram"], msg["battery"], msg["current_task_id"])

def queue_operation(task_id, input_data):
	operation_queue.append({
			"operation_id" : len(operation_queue),
			"task_id": task_id,
			"input_data": input_data,
			"status": "queued"
		})

def store_op_result(result_payload):
	operation = operation_history[len(program_history) - 1]["operations"][result_payload["operation_id"]]
	operation["result"] = result_payload["result"]
	operation["end_time_sec"] = time.time()

	for op in operation_queue:
		if op["operation_id"] == result_payload["operation_id"]:
			operation_queue.remove(op)

	worker_list[operation["worker_ip"]].available = True

	operation_history[len(program_history) - 1]["operations"][result_payload["operation_id"]] = operation

	print("Operation " +  str(result_payload["operation_id"]) + " completed successfully on worker " + str(operation["worker_ip"]) + " and returned : " + str(result_payload["result"]))

#Scheduler algorithm
#Filter all worker that are already busy
#Filter all worker that do not meet battery requirement to complete operation
#Select the one with the most computing power and ram available 
def run_operations():
	if not operation_queue:
		global waiting_for_block_completion
		waiting_for_block_completion = False
	for op in operation_queue:
		#TODO call actual algorithm
		if op["status"] != "running":
			if len(worker_list) > 0:
				worker = random.choice(list(worker_list.values()))
				if worker.current_task_id != op["task_id"]:
					worker.send_task(op["task_id"])

				worker.send_operation(op["operation_id"], op["input_data"])
				op["status"] = "running"
	 
def run_program(program_type, input_data):
	current_program_type = program_type
	global is_program_runnning 
	is_program_runnning = True

	program = programs[current_program_type]

	new_program_history_entry = {}
	new_program_history_entry["id"] = len(program_history)
	new_program_history_entry["program_name"] = program["name"]
	new_program_history_entry["status"] = "Running..."
	new_program_history_entry["input_data"] = input_data
	new_program_history_entry["start_time"] = time.time()
	program_history.append(new_program_history_entry)

	operation_history.append({ 
		"program_id": str(new_program_history_entry["id"]),
		"program_type": program_type,
		"operations": []
		})

	print("Started runnning program " 
			+ new_program_history_entry["program_name"] 
			+ " with id = " 
			+ str(new_program_history_entry["id"]))

def run_next_program_block(input_data):
	program = programs[current_program_type]
	program["program_blocks"][program["next_block"]](input_data)
	program["next_block"] += 1

	global waiting_for_block_completion
	waiting_for_block_completion = True

def end_program(result):
	global is_program_runnning
	is_program_runnning = False

	programs[current_program_type]["next_block"] = -1

	completed_program = program_history[-1]
	completed_program["result"] = result
	completed_program["status"] = "Done"
	completed_program["end_time"] = time.time()

	program_history[-1] = completed_program

	print("Program " 
		+ str(completed_program["id"]) 
		+ " (type: "
		+ completed_program["program_name"] 
		+ ") finished running with final result : " 
		+ str(result) 
		+ "   ---   Elapsed time : " 
		+ str(completed_program["end_time"] - completed_program["start_time"]) 
		+ "s")



# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, reason_code, properties):
	print(f"Connected with result code {reason_code}")
	# Subscribing in on_connect() means that if we lose the connection and
	# reconnect then subscriptions will be renewed.
	client.subscribe(system_response_topic)
	client.subscribe(operation_result_topic)

def on_disconnect(client, userdata,rc=0):
	logging.debug("Disconnected result code "+str(rc))
	client.loop_stop()

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
	payload = json.loads(msg.payload.decode('utf-8'))
	if msg.topic == system_response_topic:
		add_or_update_workers(payload)
	if msg.topic == operation_result_topic:
		store_op_result(payload)

mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqttc.on_connect = on_connect
mqttc.on_message = on_message
mqttc.on_disconnect = on_disconnect

mqttc.connect("10.10.4.160", 18883, 60)


test_time_start = time.time()

print("====================")
print("Test with Numpy dot() function : ")
random_matrix_a = np.random.randint(9, size = (10 , 10))
random_matrix_b = np.random.randint(9, size = (10 ,10))
print(random_matrix_a.dot(random_matrix_b))
test_time_end = time.time()
print("Elapsed time: " + str(round(test_time_end - test_time_start, 3)) + "s")
random_matrix_a = random_matrix_a.tolist()
random_matrix_b = random_matrix_b.tolist()
print("====================")


input_data = {"a": random_matrix_a, "b": random_matrix_b}

start_time = time.time()
mqttc.loop_start()
while True:
	time_alive_sec = round(time.time() - start_time)
	#Request system information of all worker (old and new)
	#Busy worker will 
	if time_alive_sec % 10 == 1:
		mqttc.publish(request_system_topic, payload="", qos=0, retain=False)

	if not is_program_runnning:
			run_program(1, input_data)
	else:
		if waiting_for_block_completion:
			run_operations()
		else:
			run_next_program_block(input_data)
	time.sleep(0.05)
mqttc.loop_stop()




#TODO algo ordonancement
#TODO check timeout + rescheduling
#TODO grafana
#TODO rappport
import paho.mqtt.client as mqtt
import multiprocessing
import socket
import psutil
import json
import time
import sys

if len(sys.argv) > 1:
    ip = sys.argv[1]
else:
    ip = socket.gethostbyname(socket.gethostname())

#subscriptions
request_system_topic = "pi4broker/request_system"
operation_request_topic = "pi4broker/request_operation_" + ip 
set_task_topic = "pi4broker/set_task_" + ip


#publisher
system_response_topic = "pi4broker/system_response"
operation_result_topic = "pi4broker/operation_result"

task_id = -1
task = "r = 'Error: no task loaded'"

def send_system_infos_to_brocker():
    system_info = {}
    system_info["ip"] = ip
    system_info["cpu"] = multiprocessing.cpu_count()
    system_info["ram"] = psutil.virtual_memory().available / (1024.0 **3)
    battery = psutil.sensors_battery()
    if battery:
        system_info["battery"] = -1 if battery.power_plugged else battery.secsleft
    else:
        system_info["battery"] = 0
    system_info["current_task_id"] = task_id
    mqttc.publish(system_response_topic, payload=json.dumps(system_info), qos=0, retain=False)

def start_new_operation(payload):
    data = payload["input_data"]

    #execute the task using the a and b variable as input, return the result
    r = 0
    lcls = locals()
    exec(task, globals(), lcls)
    result = lcls["r"]

    result_payload = {}
    result_payload["operation_id"] = payload["operation_id"]
    result_payload["result"] = result
    
    mqttc.publish(operation_result_topic, payload=json.dumps(result_payload), qos=0, retain=False)

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected with result code {reason_code}")

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(request_system_topic)
    client.subscribe(set_task_topic)
    client.subscribe(operation_request_topic)

    send_system_infos_to_brocker()

def on_message(client, userdata, msg):
    if msg.topic == request_system_topic:
        send_system_infos_to_brocker()
        return
    payload = json.loads(msg.payload.decode('utf-8'))
    if msg.topic == set_task_topic:
        global task_id
        global task
        task_id = payload["id"]
        task = payload["task"]
        print("Task '" + payload["name"] + "' loaded!")
    if msg.topic == operation_request_topic:
        start_new_operation(payload)

mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqttc.on_connect = on_connect
mqttc.on_message = on_message

mqttc.connect("10.10.4.160", 18883, 60)
mqttc.loop_forever()
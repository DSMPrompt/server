import paho.mqtt.client as mqtt
import time
import random
import threading

broker = "192.168.1.185"
port = 1883
topic = "shows/1/line"

pending_messages = {}
lock = threading.Lock()

def on_publish(client, userdata, mid):
    with lock:
        if mid in pending_messages:
            sent_time, message = pending_messages[mid]
            latency = (time.time() - sent_time) * 1000
            print(f"✓ ACK for '{message}' - latency: {latency:.2f}ms")
            del pending_messages[mid]

client = mqtt.Client()
client.on_publish = on_publish
client.connect(broker, port)

client.loop_start()

counter = 1

try:
    while True:
        message = str(counter)
        
        with lock:
            result = client.publish(topic, message, qos=1)
            pending_messages[result.mid] = (time.time(), message)
        
        print(f"📤 Sent: {message} (mid: {result.mid})")
        
        counter += 1
        time.sleep(random.uniform(0.5, 1.0))
        
except KeyboardInterrupt:
    print("\nStopped")
    client.loop_stop()
    client.disconnect()

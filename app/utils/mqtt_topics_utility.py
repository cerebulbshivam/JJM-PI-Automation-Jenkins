import paho.mqtt.client as mqtt
import time
import json
from .logger import setup_logger
import re
logging= setup_logger()

MQTT_BROKER = "14.99.99.166"
MQTT_PORT = 1889
MQTT_USERNAME = "MQTT"
MQTT_PASSWORD = "Mqtt@123"
MQTT_TIMEOUT = 60   # max seconds to wait for each topic

def check_multiple_topics(topics_dict) -> dict:
    """
    Check multiple MQTT topics and classify them based on their type and data content.
    
    Args:
        topics_dict: Dictionary like {'fm': [...], 'cl': [...], 'pressure': [...]}
    
    Returns:
        Dictionary with classification results for each topic
    """
    # Flatten all topics into a single list for MQTT subscription
    all_topics = []
    topic_type_map = {}
    
    for topic_type, topic_list in topics_dict.items():
        for topic in topic_list:
            all_topics.append(topic)
            topic_type_map[topic] = topic_type
    
    results = {t: {"data_found": False, "time_taken": None, "payload": None} for t in all_topics}
    start_times = {t: None for t in all_topics}
    final_results = {}
    
    # subscribe to all topics in one go
    def on_connect(client, userdata, flags, rc):
        logging.info(f"Connected with result code {rc}")
        client.subscribe([(t, 0) for t in all_topics])

    def on_message(client, userdata, msg):
        topic = msg.topic
        try:
            payload = msg.payload.decode('utf-8', errors='replace')
        except Exception as e:
            logging.warning(f"Error decoding message from {topic}: {e}")
            payload = msg.payload.hex()  # fallback to hexadecimal representation
            
        if not results[topic]["data_found"]:  # only record first message
            results[topic]["data_found"] = True
            results[topic]["time_taken"] = round(time.time() - start_times[topic], 2)
            try:
                results[topic]["payload"] = json.loads(payload)
                final_results[topic] = results[topic]["payload"]
            except Exception as e:
                logging.warning(f"Error parsing JSON from {topic}: {e}")
                results[topic]["payload"] = payload
            logging.info(f"Received from {topic} in {results[topic]['time_taken']}s")

    client = mqtt.Client()
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_start()

        # start timers per topic
        for t in all_topics:
            start_times[t] = time.time()

        # wait until timeout or all topics received data
        global_start = time.time()
        try:
            while (time.time() - global_start) < MQTT_TIMEOUT:
                if all(r["data_found"] for r in results.values()):
                    break
                time.sleep(0.1)
        except KeyboardInterrupt:
            logging.info("Received keyboard interrupt, cleaning up...")
        except Exception as e:
            logging.error(f"Error during MQTT operation: {e}")
        finally:
            # Ensure we always clean up the MQTT client
            try:
                client.loop_stop()
                client.disconnect()
            except Exception as e:
                logging.error(f"Error during MQTT cleanup: {e}")

        # fill missing topics with timeout values
        for t, res in results.items():
            if not res["data_found"]:
                res["time_taken"] = MQTT_TIMEOUT
    except Exception as e:
        logging.error(f"Failed to connect to MQTT broker: {e}")
        # Return empty results in case of connection failure
        for t, res in results.items():
            if not res["data_found"]:
                res["time_taken"] = MQTT_TIMEOUT
    
    # logging.info(f"MQTT topic check complete. with True status {final_results}")
    
    # Now classify the results based on topic type and content
    classified_results = classify_mqtt_results(results, topic_type_map)
    c = [] 
    n = []
    e = []
    u = []
    for uid, r in classified_results.items():
        if r["status"] == "communicated":
            c.append(uid)
        elif r["status"] == "not_communicated":
            n.append(uid)
        elif r["status"] == "error":
            e.append(uid)
        elif r["status"] == "unknown":
            u.append(uid)
    
    # print(f"Final result is : {result}")
    print(f"Summary: Communicated={len(c)}, Not Communicated={len(n)}, Error={len(e)}")
    print(f"Communicated topics: {c}")
    print(f"Not Communicated topics: {n}")
    print(f"Error topics: {e}")
    print(f"Unkown topics: {u}")
    return classified_results

def classify_mqtt_results(results, topic_type_map):
    """
    Classify MQTT results based on topic type and data content
    """
    classification_results = {}
    
    for topic, result in results.items():
        topic_type = topic_type_map.get(topic, 'unknown')
        payload = result.get("payload")
        data_found = result.get("data_found", False)
        # if topic == '861657074890273':
        #     print(f"=====Debug: Topic {topic} has payload {payload} and data_found={data_found}")
            # flow_error = payload.get('Flow_Error')
            # print(f"=====Debug: Flow_Error for topic {topic} is {flow_error}")
        if topic_type == 'fm':
            status = classify_fm_topic(data_found, payload)
        elif topic_type == 'pressure':
            status = classify_pressure_topic(data_found, payload)
        elif topic_type == 'cl':
            status = classify_cl_topic(data_found, payload)
        else:
            status = 'unknown_type'
        
        classification_results[topic] = {
            'type': topic_type,
            'status': status,
            'data_found': data_found,
            'payload': payload,
            'time_taken': result.get('time_taken')
        }
    
    return classification_results

def classify_fm_topic(data_found, payload):
    """
    Case 1 - FM classification:
    - If data exists and Flow_Error is 1 → error
    - If no data → not_communicated  
    - If data exists and Flow_Error is 0 → communicated
    """
    if not data_found or payload is None:
        return 'not_communicated'
    
    if isinstance(payload, dict):
        # Define possible key variations
        possible_keys = [
            'Flow_Error', 'Flow_error', 'flow_error', 'FLOW_ERROR',
            'FlowError', 'flowError', 'flow-error', 'flow.error'
        ]
        flow_error = None
        found_key = None
        
        # Try each possible key
        for key in possible_keys:
            if key in payload:
                flow_error = payload.get(key)
                found_key = key
                break
        
        # flow_error = payload.get('Flow_Error') Flow_error, 
        if type(flow_error) == str:
            flow_error = flow_error.strip()
            flow_error = int(flow_error) if flow_error.isdigit() else flow_error
            print(f'str to int converted {flow_error}')
            
        if flow_error == 1:
            return 'error'
        elif flow_error == 0:
            return 'communicated'
        else:
            # Flow_Error not found or invalid value
            return 'unknown'
    else:
        # Payload is not a dictionary (couldn't parse JSON)
        return 'error'

def classify_pressure_topic(data_found, payload):
    """
    Case 2 - Pressure classification:
    - If data found for topic → communicated
    - If no data found → not_communicated
    """
    if data_found and payload is not None:
        return 'communicated'
    else:
        return 'not_communicated'

def classify_cl_topic(data_found, payload):
    """
    Case 3 - CL classification:
    - If Cl_Error is 0 → communicated
    - If no data response → not_communicated
    - If Cl_Error is 1 → check AI1:
        - If AI1 found and >= 0 → communicated
        - Else → error
    """
    if not data_found or payload is None:
        return 'not_communicated'
    
    if isinstance(payload, dict):
        # Define possible key variations
        possible_keys = [
            'Cl_Error', 'Cl_error', 'cl_error', 'CL_Error','CL_ERROR',
            'ClError', 'clerror', 'cl-error', 'cl.error'
        ]
        cl_error = None
        found_key = None
        
        # Try each possible key
        for key in possible_keys:
            if key in payload:
                cl_error = payload.get(key)
                found_key = key
                break
        
        if type(cl_error) == str:
            cl_error = cl_error.strip()
            cl_error = int(cl_error) if cl_error.isdigit() else cl_error
            
        if cl_error == 0:
            return 'communicated'
        elif cl_error == 1:
            # Check AI1 value
            ai1_value = payload.get('AI1')
            if type(ai1_value) == str:
                ai1_value = ai1_value.strip()
                ai1_value = float(ai1_value) if re.match(r"^-?\d+(\.\d+)?$", ai1_value) else None
            
            if ai1_value is not None and ai1_value >= 0:
                return 'communicated'
            else:
                return 'error'
        else:
            # Cl_Error not found or invalid value
            return 'unknown'
    else:
        # Payload is not a dictionary (couldn't parse JSON)
        return 'error'

def get_status_summary(classification_results):
    """
    Generate a summary of communication status grouped by status type
    """
    summary = {
        'communicated': [],
        'not_communicated': [],
        'error': [],
        'unknown_type': []
    }
    
    for topic, result in classification_results.items():
        status = result['status']
        summary[status].append({
            'topic': topic,
            'type': result['type'],
            'time_taken': result['time_taken']
        })
    
    return summary

def get_type_summary(classification_results):
    """
    Generate a summary grouped by topic type (fm, cl, pressure)
    """
    type_summary = {}
    
    for topic, result in classification_results.items():
        topic_type = result['type']
        if topic_type not in type_summary:
            type_summary[topic_type] = {
                'communicated': [],
                'not_communicated': [],
                'error': [],
                'unknown_type': []
            }
        
        status = result['status']
        type_summary[topic_type][status].append({
            'topic': topic,
            'time_taken': result['time_taken']
        })
    
    return type_summary

# Example usage:
"""
topics = {
    'fm': ['fm_topic_1', 'fm_topic_2'],
    'cl': ['cl_topic_1', 'cl_topic_2'], 
    'pressure': ['pressure_topic_1']
}

# Get classified results
results = check_multiple_topics(topics)

# Print individual results
for topic, result in results.items():
    print(f"{topic} ({result['type']}): {result['status']} - {result['time_taken']}s")

# Get summaries
status_summary = get_status_summary(results)
type_summary = get_type_summary(results)

print("\n--- Status Summary ---")
for status, topics_list in status_summary.items():
    if topics_list:
        print(f"{status.upper()}: {len(topics_list)} topics")

print("\n--- Type Summary ---")
for topic_type, statuses in type_summary.items():
    print(f"\n{topic_type.upper()}:")
    for status, topics_list in statuses.items():
        if topics_list:
            print(f"  {status}: {len(topics_list)} topics")
"""

# def check_multiple_topics(topics) -> dict:
#     """
#     Subscribe to multiple MQTT topics and return which ones have data + response time.
#     Returns dict where key is topic and value is dict with:
#     - data_found: bool - whether data was received
#     - time_taken: float - seconds taken to receive data or timeout
#     - payload: dict/str - the message payload if received
#     """
#     # Filter out empty/None topics
#     # topics = [t for t in topics if t and not str(t).endswith('.0')]
#     # if not topics:
#     #     return {}

#     #  now I will be have topics like {'fm': [...], 'cl': [...], 'pressure': [...]}
    
#     results = {t: {"data_found": False, "time_taken": None, "payload": None} for t in topics}
#     start_times = {t: None for t in topics}
#     final_results = {}
#     def on_connect(client, userdata, flags, rc):
#         logging.info(f"Connected with result code {rc}")
#         # subscribe to all topics in one go
#         client.subscribe([(t, 0) for t in topics])

#     def on_message(client, userdata, msg):
#         topic = msg.topic
#         try:
#             payload = msg.payload.decode('utf-8', errors='replace')
#         except Exception as e:
#             logging.warning(f"Error decoding message from {topic}: {e}")
#             payload = msg.payload.hex()  # fallback to hexadecimal representation
            
#         if not results[topic]["data_found"]:  # only record first message
#             results[topic]["data_found"] = True
#             results[topic]["time_taken"] = round(time.time() - start_times[topic], 2)
#             try:
#                 results[topic]["payload"] = json.loads(payload)
#                 final_results[topic] = results[topic]["payload"]
#             except Exception as e:
#                 logging.warning(f"Error parsing JSON from {topic}: {e}")
#                 results[topic]["payload"] = payload
#             logging.info(f"Received from {topic} in {results[topic]['time_taken']}s: {results[topic]['payload']}")

#     client = mqtt.Client()
#     client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
#     client.on_connect = on_connect
#     client.on_message = on_message

#     try:
#         client.connect(MQTT_BROKER, MQTT_PORT, 60)
#         client.loop_start()

#         # start timers per topic
#         for t in topics:
#             start_times[t] = time.time()

#         # wait until timeout or all topics received data
#         global_start = time.time()
#         try:
#             while (time.time() - global_start) < MQTT_TIMEOUT:
#                 if all(r["data_found"] for r in results.values()):
#                     break
#                 time.sleep(0.1)
#         except KeyboardInterrupt:
#             logging.info("Received keyboard interrupt, cleaning up...")
#         except Exception as e:
#             logging.error(f"Error during MQTT operation: {e}")
#         finally:
#             # Ensure we always clean up the MQTT client
#             try:
#                 client.loop_stop()
#                 client.disconnect()
#             except Exception as e:
#                 logging.error(f"Error during MQTT cleanup: {e}")

#         # fill missing topics with timeout values
#         for t, res in results.items():
#             if not res["data_found"]:
#                 res["time_taken"] = MQTT_TIMEOUT
#     except Exception as e:
#         logging.error(f"Failed to connect to MQTT broker: {e}")
#         # Return empty results in case of connection failure
#         for t, res in results.items():
#             if not res["data_found"]:
#                 res["time_taken"] = MQTT_TIMEOUT
#     logging.info(f"MQTT topic check complete. with True status {final_results}")
#     return results


# if __name__ == "__main__":
#     test_topics = ["861657074697637", "861657074697645", "861657074697650"]
#     results = check_multiple_topics(test_topics)
#     print("\n=== Final Results ===")
#     for topic, data in results.items():
#         print(topic, "=>", data)

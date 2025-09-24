# # from pi_tag_utility import create_pi_tag, create_pi_multiple_tag
# from app.utils.mqtt_topics_utility import check_mqtt_data_for_topic

# # print("--- Testing PI Tag Creation Utility ---")
# # create_pi_multiple_tag(["TEST_TAG_AMRAVATI_7", "TEST_TAG_AMRAVATI_9"], "Amravati")
import asyncio
from app.utils.mqtt_topics_utility import check_multiple_topics

async def main():
    print("--- Testing MQTT Topic Utility ---")
    # topics = ["861657074697637", "861657074697638", "861657074697639", "861657074697640", "861657074697641", "861657074697642", "861657074697643", "861657074697644", "861657074697645", "861657074697646"]
    topics = {
        "fm": [
            "868768068072415",
            "868768068067811",
            "868768068058448",
            "863597073736499",
            "868797073748890",
            "863597073740194",
            "868768068068843",
            "868768068066771",
            "868768068065518",
            "868768068067530"
        ],
        "cl": [
            "862360073407418",
            "862360073422227",
            "861657074694246",
            "861657074699021",
            "861657074641585",
            "861657074890273",
            "861657074672580"
        ],
        "pressure": [
            "240112185",
            "240111745",
            "240111963",
            "240111626",
            "240111769",
            "240112102",
            "240111694",
            "240112184"
        ]
    }

    result = check_multiple_topics(topics)
    # result is like 
    # {'868768068072415': {'type': 'fm', 'status': 'communicated', 'data_found': True, 'payload': {'UID': '868768068072415', 'AI1': '0.00', 'AI2': '0.00', 'CLOCK': '2025-09-23 15:55:12', 'CSQ': '24,99', 'Soft.Ver.': 'V29032025', 'Type': 'Adept', 'Flow': '0.0', 'Total': '10585', 'Dir': '1', 'Flow_Error': '0', 'Cl_Error': '1'}, 'time_taken': 3.09}}
    # print(f"Total topics requested: {sum(len(v) for v in topics.values())}")
    # print(f"Total topics processed: {len(result)}")
    
    c = [] 
    n = []
    e = []
    for uid, r in result.items():
        if r["status"] == "communicated":
            c.append(uid)
        elif r["status"] == "not_communicated":
            n.append(uid)
        elif r["status"] == "error":
            e.append(uid)
    
    # print(f"Final result is : {result}")
    print(f"Summary: Communicated={len(c)}, Not Communicated={len(n)}, Error={len(e)}")
    print(f"Communicated topics: {c}")
    print(f"Not Communicated topics: {n}")
    print(f"Error topics: {e}")
    
if __name__ == "__main__":
    asyncio.run(main())

# # create_pi_tag("TEST_TAG_AMRAVATI_11", "Amravati")


# import paho.mqtt.client as mqtt

# topics = ["861657074697637", "861657074697638", "861657074697639", "861657074697640", "861657074697641", "861657074697642", "861657074697643", "861657074697644", "861657074697645", "861657074697646"]

# def on_message(client, userdata, msg):
#     print(f"Received from {msg.topic}: {msg.payload.decode()}")

# client = mqtt.Client()
# client.username_pw_set("MQTT", "Mqtt@123")
# client.on_message = on_message
# client.connect("14.99.99.166", 1889, 60)

# # Subscribe to multiple topics at once
# client.subscribe([(t, 0) for t in topics])

# client.loop_forever()

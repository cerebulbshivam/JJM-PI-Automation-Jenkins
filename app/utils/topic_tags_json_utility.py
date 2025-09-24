from app.utils.utils import normalize_other_topic_id, normalize_pressure_topic_id, validate_reservoirs
from collections import defaultdict
import os
from dotenv import load_dotenv
# Load environment variables from .env
load_dotenv()

import json
from .logger import setup_logger
# Configure logging
logging = setup_logger()


PUNE_PRESSURE_JSON_FILE = os.getenv('PUNE_PRESSURE_JSON_FILE')
PRESSURE_JSON_FILE = os.getenv('PRESSURE_JSON_FILE')
PUNE_JSON_FILE = os.getenv('PUNE_JSON_FILE')
TAGS_JSON_FILE = os.getenv('TAGS_JSON_FILE')


def chlorine_tags_json_utility(df_verif, current_region, chlorine_tags):
    
    cl_mqtt_topics = defaultdict(list)
    if "Topic For CL" in df_verif.columns and "Reservoir" in df_verif.columns:
        for topic, reservoir in zip(df_verif["Topic For CL"].dropna(), df_verif["Reservoir"].dropna()):
            norm_topic = normalize_other_topic_id(topic)
            cl_mqtt_topics[norm_topic].append(reservoir)
        
    else:
        cl_mqtt_topics = {}

    # convert defaultdict back to normal dict
    cl_mqtt_topics = dict(cl_mqtt_topics)

    
    logging.info(f"Current Region for chlorine tags: {current_region}")
    pressure_json_path = PUNE_JSON_FILE if current_region.strip().lower() == "pune" else TAGS_JSON_FILE
    if os.path.exists(pressure_json_path):
        with open(pressure_json_path, "r") as f:
            try:
                existing_cl_data = json.load(f)
            except Exception:
                existing_cl_data = {}
    else:
        existing_cl_data = {}

    
    for item in chlorine_tags:
        topic = normalize_other_topic_id(item["topic"])
        cl_tag = item.get("cl_tag", "")
        cl_error_tag = item.get("cl_error_tag", "")
        cl_type = item.get("cl_type", "")
        
        if topic in cl_mqtt_topics.keys():

            # if topic in existing_cl_data:
            #     del existing_cl_data[topic]
            if current_region.strip().lower() == "pune":
                
                # Case 1: Multiple reservoirs > maintain lists
                reservoirs = cl_mqtt_topics[topic]
                if len(reservoirs) > 1:
                    reservoirs = validate_reservoirs(cl_mqtt_topics[topic])
                    
                    if topic not in existing_cl_data:
                        existing_cl_data[topic] = {"Chlorine": [], "Cl_error": []}

                    if cl_tag not in existing_cl_data[topic]["Chlorine"]:
                        existing_cl_data[topic]["Chlorine"].append(cl_tag)
                    if cl_error_tag not in existing_cl_data[topic]["Cl_error"]:
                        existing_cl_data[topic]["Cl_error"].append(cl_error_tag)

                # Case 2: Single reservoir
                else:

                    if current_region.strip().lower() == "pune":
                        tag_data = {
                            "Chlorine": cl_tag,
                            "Cl_error": cl_error_tag,
                        }
                    else:
                        if cl_type.lower().strip().replace(" ", "") == "rs485":
                            tag_data = {
                                "Cl": cl_tag,
                                "Cl_Error": cl_error_tag,
                            }
                        else:
                            tag_data = {
                                "AI1": cl_tag,
                                "Cl_Error": cl_error_tag,
                            }
                    existing_cl_data[topic] = tag_data
            else:
                if cl_type.lower().strip().replace(" ","") == "rs485":
                    tag_data = {
                        "Cl": cl_tag,
                        "Cl_Error": cl_error_tag
                        }
                else:
                    tag_data = {
                        "AI1": cl_tag,
                        "Cl_Error": cl_error_tag
                        }
                existing_cl_data[topic] = tag_data

    with open(pressure_json_path, "w") as f:
        json.dump(existing_cl_data, f, indent=2)

def fl_tags_json_utility(df_verif,current_region, flow_meter_tags):

    if "Topic For Flow Meter" in df_verif.columns:
        fl_mqtt_topics = {
            normalize_other_topic_id(topic)  # apply normalization
            for topic in df_verif["Topic For Flow Meter"].dropna().unique()
        }
    else:
        fl_mqtt_topics = set()
    
    # If current_region is "Pune" then it will be written in PUNE_PRESSURE_JSON_FILE
    logging.info(f"Current Region for tags : {current_region}")
    pressure_json_path = PUNE_JSON_FILE if current_region.strip().lower() == "pune" else TAGS_JSON_FILE
    if os.path.exists(pressure_json_path):
        with open(pressure_json_path, "r") as f:
            try:
                existing_fl_data = json.load(f)
            except Exception:
                existing_fl_data = {}
    else:
        existing_fl_data = {}
    for item in flow_meter_tags:
        topic = normalize_other_topic_id(item["topic"])
        fl_rate_tag = item.get("fl_rate_tag", "")
        total_fl_tag = item.get("total_fl_tag", "")
        sen_err_fl_mtr_tag = item.get("sen_err_fl_mtr_tag", "")
        
        if topic in fl_mqtt_topics:
            if topic in existing_fl_data:
                del existing_fl_data[topic]
            if current_region.strip().lower() == "pune":
                tag_data = {
                    "Volume_Flow": fl_rate_tag,
                    "Positive_Totalizer": total_fl_tag,
                    "Flow_error": sen_err_fl_mtr_tag
                    }
                existing_fl_data[topic] = tag_data
            else:
                tag_data = {
                    "Flow": fl_rate_tag,
                    "Total": total_fl_tag,
                    "Flow_error": sen_err_fl_mtr_tag
                    }
                existing_fl_data[topic] = tag_data
    with open(pressure_json_path, "w") as f:
        json.dump(existing_fl_data, f, indent=2)

def pressure_tags_json_utility(df_verif, current_region, pressure_tags):
    if "Topic For Pressure" in df_verif.columns:
        mqtt_topics = {
            normalize_pressure_topic_id(topic, current_region)  # apply normalization
            for topic in df_verif["Topic For Pressure"].dropna().unique()
        }
    else:
        mqtt_topics = set()
    
    # If current_region is "Pune" then it will be written in PUNE_PRESSURE_JSON_FILE
    logging.info(f"=====>>>>>Row Region='{current_region}',  Writing to={'PUNE' if current_region.strip().lower() == 'pune' else 'OTHER'}")

    pressure_json_path = PUNE_PRESSURE_JSON_FILE if current_region.strip().lower() == "pune" else PRESSURE_JSON_FILE
    if os.path.exists(pressure_json_path):
        with open(pressure_json_path, "r") as f:
            try:
                existing_data = json.load(f)
            except Exception:
                existing_data = {}
    else:
        existing_data = {}
    for item in pressure_tags:
        topic = normalize_pressure_topic_id(item["topic"], current_region)
        tag = item["tag"]
        if topic in mqtt_topics:
            if topic in existing_data:
                del existing_data[topic]
            existing_data[topic] = tag
    with open(pressure_json_path, "w") as f:
        json.dump(existing_data, f, indent=2)
import pandas as pd
import re
# from app.utils.websocket_manager import broadcast_message
# A list of columns that are expected to be numeric (float or int)
NUMERIC_COLS = [
    "Depth_of_Well", "Diameter_of_well", "Pump_House_Location_Geotag_Latitude",
    "Pump_House_Location_Geotag_Longitude", "Numbers_of_Pumps", "Population",
    "Household", "Number_of_ESR", "Number_of_MBR", "Number_of_GSR",
    "Reservoir_Capacity", "Reservoir_Geotag_Latitude", "Reservoir_Geotag_Longitude",
    "Inlet_Line_Size", "Inlet_Rising_Main_Line_Size", "Number_of_Outlet",
    "Outlet_Line_Size", "Outlet_Distribution_Line_Size",
    "Outlet_Distribution_Line_Material", "Availability_of_Isolation_Valve",
    "Geo_Location_of_Flow_Meter_Latitude", "Geo_Location_of_Flow_Meter_Longitude",
    "Geo_Location_of_RCA_Latitude", "Geo_Location_of_RCA_Longitude",
    "Number_of_Distribution", "Critical_Pressure_Point_Location_Latitude",
    "Critical_Pressure_Point_Location_Longitude", "Distribution_Line_Size",
    "Number_of_Reservoir", "Reservoir_Capacity_Val",
    "Reservoir_Level_Population", "Reservoir_Level_Household", "ID"
]

def clean_value(val, col_name):
    """Safely converts a value to the appropriate type, returning None on failure."""
    if pd.isna(val) or (isinstance(val, str) and val.strip().lower() in ["null", "under construction.", "will be updated later", ""]):
        return None
    if col_name in NUMERIC_COLS:
        try:
            return float(val)
        except (ValueError, TypeError):
            return None
    return val

def normalize_reservoir(value: str) -> str:
    if pd.isna(value):
        return ""
    val = str(value).upper().strip()           # uppercase + strip
    val = val.replace("-", "")                 # remove hyphens
    val = val.replace(".", "")                 # remove hyphens
    val = val.replace(" ", "")                 # remove spaces
    val = val.replace("OL", "OUTLET")          # OL → OUTLET
    return val

def derive_base_reservoir(name):
    """
    Extracts base reservoir name from 'Name_of_the_Reservoir'
    Example:
      - "Existing 2 LL ESR- Outlet-2" -> "Existing 2 LL ESR"
      - "Proposed 0.85 LL MBR-Outlet-3" -> "Proposed 0.85 LL MBR"
      - "Existing 0.20 LL ESR-OL1" -> "Existing 0.20 LL ESR"
    """
    if not name or pd.isna(name):
        return None
    # remove suffix like -Outlet-2, -Outlet-3, -OL1 etc.
    return re.sub(r"[-\s]*(Outlet[-\s]*\d+|OL[-\s]*\d+)$", "", str(name).strip(), flags=re.IGNORECASE)


def normalize_pressure_topic_id(topic_id: str, region : str) -> str:
    """
    Normalizes the topic ID by:
    - Converting to uppercase
    - Replacing slashes (/) with underscores (_)
    - Removing any leading or trailing whitespace
    - Removing .0 suffix if present
    """
    # Convert to string and normalize
    topic_id = str(topic_id).strip().replace(" ", "").upper()
    
    # Remove .0 suffix if present
    if topic_id.endswith(".0"):
        topic_id = topic_id[:-2]
        
    # Add leading 0 if needed
    if region.lower() != "pune" and not topic_id.startswith("0"):
        topic_id = "0" + topic_id
        
    return topic_id

def normalize_other_topic_id(topic_id: str) -> str:
    """
    Normalizes topic IDs for flow meter and chlorine sensors by:
    - Converting to uppercase
    - Removing whitespace
    - Removing .0 suffix if present
    """
    topic_id = str(topic_id).strip().replace(" ", "").upper()
    
    # Remove .0 suffix if present
    if topic_id.endswith(".0"):
        topic_id = topic_id[:-2]
        
    return topic_id

def validate_reservoirs(reservoirs):
    pattern = re.compile(
        r'(?:\bOL\s*-?\s*\d+|\bOutlet\s*-?\s*\d+)',
        re.IGNORECASE
    )
    valid_reservoirs = []
    for res in reservoirs:
        if pattern.search(res.strip()):   # <-- use search, not match
            valid_reservoirs.append(res)
    return valid_reservoirs

async def send_log(message: str):
    """Send a log message to all connected WebSocket clients"""
    print(message)
    # await broadcast_message(message)
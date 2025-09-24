
import sys
import clr
import os
from dotenv import load_dotenv
from app.utils.utils import send_log

# Load environment variables from .env
load_dotenv()

sys.path.append(os.getenv('PI_SYS_PATH'))  
clr.AddReference(f"{os.getenv('PI_SDK_REFRENCE')}")  


import System
from System.Collections.Generic import Dictionary,List

from OSIsoft.AF import *
from OSIsoft.AF.PI import *
from OSIsoft.AF.Asset import *
from OSIsoft.AF.Data import *
from OSIsoft.AF.Time import *
from OSIsoft.AF.UnitsOfMeasure import *


from .logger import setup_logger
# from PIconnect import PIServer




# Configure logging
logging = setup_logger()

REGION_POINT_SOURCE_MAP = {
    "amravati": "AU",
    "nagpur": "NA",
    "chhatrapati sambhajinagar": "CS",
    "nashik": "NASHIK",
    "pune": "PUNE",
    "konkan": "KONKAN",
}

async def create_pi_multiple_tag(tag_names: list[str], region: str, server_name: str = f"{os.getenv('PI_SERVER')}"):
    """
    Creates multiple AVEVA PI tags using PIConnect.

    Args:
        tag_names (list[str]): List of PI tag names to create.
        region (str): The region to determine the Point Source.
        server_name (str): The IP address or hostname of the PI Server.
    """
    result = {'created': [], 'skipped': [], 'errors': []}
    
    try:
        # 1. Connect to the PI Server
        piServers = PIServers()
        server = piServers[server_name]

        logging.info(f"Successfully connected to PI Server: {server_name} Server: {server}")
        # await send_log(f"Successfully connected to PI Server: {server_name} Server: {server}")

        # 2. Determine Point Source based on region
        point_source = REGION_POINT_SOURCE_MAP.get(region.strip().lower())
        if not point_source:
            logging.error(f"Invalid region '{region}'. Point Source not found in map.")
            # await send_log(f"Error : Invalid region '{region}'. Point Source not found in map.")
            result['errors'].append(f"Invalid region '{region}'. Point Source not found in map.")
            return result
        
        # 3. Filter out existing tags
        tags_to_create = []
        tags_skipped = []
        for tag in tag_names:
            try:
                existing_point = PIPoint.FindPIPoint(server, tag)
                logging.warning(f"Tag '{tag}' already exists. Skipping creation.")
                # await send_log(f"Tag '{tag}' already exists. Skipping creation.")
                result['skipped'].append(tag)
                tags_skipped.append(tag)
            except Exception:
                # Tag does not exist, add to creation list
                tags_to_create.append(tag)

        if not tags_to_create:
            logging.info("No new tags to create. All tags already exist.")
            # await send_log("Warning : No new tags to create. All tags already exist.")
            return result
        
        # 4. Define tag attributes (shared for all tags)
        attributes = Dictionary[str, System.Object]()
        attributes["PointSource"] = point_source
        attributes["PointType"] = PIPointType.Float64
        attributes["compdev"] = 0.0
        attributes["excdev"] = 0.0

        # 4. Create the new PI Points
        try:
            # Convert Python list to .NET List
            net_tag_names = List[str]()
            for tag in tags_to_create:
                net_tag_names.Add(tag)

            # Create multiple PI Points
            results = server.CreatePIPoints(net_tag_names, attributes)
            for tag in tags_to_create:
                result['created'].append(tag)
                # await send_log(f"PI Tag '{tag}' created successfully.")
                
        except Exception as e:
            result['errors'].append(str(e))
            
        logging.info(f"Successfully created PI tags {tags_to_create} with Point Source '{point_source}' on server '{server_name}'.\nSkipped existing tags: {tags_skipped}")
        
    except Exception as e:
        result['errors'].append(str(e))
    
    return result


async def create_pi_tag(tag_name: str, region: str, server_name: str = "{os.getenv('PI_SERVER')}"):
    """
    Creates an AVEVA PI tag using PIConnect.

    Args:
        tag_name (str): The name of the PI tag to create.
        region (str): The region to determine the Point Source.
        server_name (str): The IP address or hostname of the PI Server.
    """
    try:
        # 1. Connect to the PI Server

        piServers = PIServers()  
        server = piServers['192.168.1.115']


        logging.info(f"Successfully connected to PI Server: {server_name} Server: {server}", )

        # 2. Determine Point Source based on region
        point_source = REGION_POINT_SOURCE_MAP.get(region)
        if not point_source:
            logging.error(f"Invalid region '{region}'. Point Source not found in map.")
            return False

        # 3. Check if tag already exists
        try:
            existing_point = PIPoint.FindPIPoint(server, tag_name)
            if existing_point:
                logging.warning(f"Tag '{tag_name}' already exists. Skipping creation.")
                return False
        except Exception:
            # Exception means tag does not exist, so continue to create
            pass

        # 4. Define tag attributes
        attributes = Dictionary[str, System.Object]()
        attributes["PointSource"] = point_source
        attributes["PointType"] = PIPointType.Float64
        attributes["compdev"] = 0.0
        attributes["excdev"] = 0.0 
        attributes["Descriptor"] = "Test Tag"

        # 5. Create the new PI Point
        try:
            pi_point = server.CreatePIPoint(tag_name, attributes)
            print(f"PI Point {tag_name} created successfully")
        except Exception as e:
            print(f"Error creating PI Point {tag_name}: {e}")

        logging.info(f"Successfully created PI tag '{tag_name}' with Point Source '{point_source}' on server '{server_name}'.")
        return True

    except Exception as e:
        logging.error(f"Error creating PI tag '{tag_name}': {e}")
        return False

if __name__ == "__main__":
    import asyncio
    
    async def run_tests():
        # Example usage for standalone testing
        print("--- Testing PI Tag Creation Utility (PIConnect) ---")

        # Test Case 1: Successful creation
        print("\nAttempting to create 'TEST_TAG_AMRAVATI_PI' in Amravati...")
        success = await create_pi_tag("TEST_TAG_AMRAVATI_PI", "Amravati")
        print(f"Creation successful: {success}")

        # Test Case 2: Tag already exists (should return False)
        print("\nAttempting to create 'TEST_TAG_AMRAVATI_PI' again (should fail as it exists)...")
        success = await create_pi_tag("TEST_TAG_AMRAVATI_PI", "Amravati")
        print(f"Creation successful: {success}")

        # Test Case 3: Invalid region
        print("\nAttempting to create 'TEST_TAG_INVALID_PI' with an invalid region...")
        success = await create_pi_tag("TEST_TAG_INVALID_PI", "InvalidRegion")
        print(f"Creation successful: {success}")

        # Test Case 4: Another successful creation
        print("\nAttempting to create 'TEST_TAG_NAGPUR_PI' in Nagpur...")
        success = await create_pi_tag("TEST_TAG_NAGPUR_PI", "Nagpur")
        print(f"Creation successful: {success}")

    # Run the async tests
    asyncio.run(run_tests())
import requests
import json
import uuid
from datetime import datetime
import boto3
from fastapi import HTTPException

# Initialize AWS resources
dynamodb = boto3.resource('dynamodb')
error_log_table = dynamodb.Table("Test_ErrorLog")

INSPIRE_BASE_URL = "https://ihweb.afi-uplift.co.uk/insphire.corporate/api"

def log_error_to_dynamodb(event_source, error_message, payload=None):
    """Logs errors into the DynamoDB error log table."""
    try:
        error_log_table.put_item(Item={
            "id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "error_msg": error_message,
            "payload": json.dumps(payload) if payload else "",
            "source_name": event_source
        })
    except Exception as db_error:
        print(f"Failed to log error to DynamoDB: {db_error}")

def get_session():
    """Authenticate and get a session ID from Insphire."""
    url = f"{INSPIRE_BASE_URL}/sessions/logon"
    payload = {
        "USERNAME": "WEBQUOTES",
        "PASSWORD": "BSbm5R",
        "DEPOT": "SUP"
    }
    
    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()  # Raise exception for HTTP errors
        
        session_data = response.json()
        return session_data.get("SESSIONID")
    
    except requests.HTTPError as e:
        log_error_to_dynamodb("insphire", f"HTTP Error: {e.response.text}", payload)
        raise HTTPException(status_code=e.response.status_code, detail="Failed to authenticate with Insphire")
    
    except Exception as e:
        log_error_to_dynamodb("insphire", str(e), payload)
        raise HTTPException(status_code=500, detail="An unexpected error occurred while authenticating with Insphire")



# import requests
# from fastapi import HTTPException

# INSPIRE_BASE_URL = "https://ihweb.afi-uplift.co.uk/insphire.corporate/api"

# def get_session():
#     """Authenticate and get a session ID from Insphire."""
#     url = f"{INSPIRE_BASE_URL}/sessions/logon"
#     response = requests.post(url, data={
#         "USERNAME": "WEBQUOTES",
#         "PASSWORD": "BSbm5R",
#         "DEPOT": "SUP"
#     })
#     if response.status_code == 200:
#         session_data = response.json()
#         return session_data.get("SESSIONID")
#     else:
#         raise HTTPException(status_code=500, detail="Failed to authenticate with Insphire")

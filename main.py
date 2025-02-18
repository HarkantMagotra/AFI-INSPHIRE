from fastapi import FastAPI, HTTPException
import requests
import base64

app = FastAPI()

# Constants
INSPIRE_BASE_URL = "https://ihweb.afi-uplift.co.uk/insphire.corporate/api"
MOENGAGE_EVENT_URL = "https://api-02.moengage.com/v1/event/6978DCU8W19J0XQOKS7NEE1C_DEBUG"
MOENGAGE_USERNAME = "6978DCU8W19J0XQOKS7NEE1C_DEBUG"
MOENGAGE_PASSWORD = "8bZOSpK7Q9hE9xruwrNYGBhu"

@app.get("/fetch_contract_data/")
async def fetch_contract_data(contno: str, jobtype: str):
    """Fetch contract details, process data, and send an event to MoEngage."""
    try:
        session_id = get_session()
        contract_items = fetch_contract_items_and_codes(contno, session_id)
        contract_details = fetch_contract_details(contno, session_id)
        
        if not contract_details:
            raise HTTPException(status_code=404, detail="No contract details found")
        
        attributes = format_attributes(contract_items, contract_details, jobtype)
        
        # Send event to MoEngage
        customer_id = contract_details.get("ORDBYEMAIL", "unknown@example.com")
        send_event_to_moengage(customer_id, jobtype, attributes)
        
        return {"status": "Event sent successfully to MoEngage"}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


def get_session():
    """Authenticate with Insphire and return session ID."""
    response = requests.post(f"{INSPIRE_BASE_URL}/sessions/logon", data={
        "USERNAME": "WEBQUOTES",
        "PASSWORD": "BSbm5R",
        "DEPOT": "SUP"
    })
    
    if response.status_code == 200:
        return response.json().get("SESSIONID")
    else:
        raise Exception("Failed to authenticate with Insphire")


def fetch_contract_items_and_codes(contno: str, session_id: str):
    """Fetch contract items along with their analysis codes."""
    response = requests.get(
        f"{INSPIRE_BASE_URL}/contractitems?api_key={session_id}&$filter=CONTNO eq '{contno}'"
        "&fields=ITEMNO,RATECODE,RATE1,HIREDATE,ESTRETD,DEPOT,INSURANCE,CONTNO,ITEMDESC3"
    )
    
    if response.status_code != 200:
        raise Exception("Contract items not found")
    
    contract_items = response.json()
    for item in contract_items:
        item_no = item.get("ITEMNO")
        if item_no:
            item['ANLCODE'] = fetch_analysis_code(item_no, session_id)
    
    return contract_items


def fetch_analysis_code(item_no: str, session_id: str):
    """Fetch the analysis code for a given item number."""
    response = requests.get(f"{INSPIRE_BASE_URL}/stock/{item_no}?api_key={session_id}&fields=ANLCODE")
    return response.json().get("ANLCODE") if response.status_code == 200 else None


def fetch_contract_details(contno: str, session_id: str):
    """Fetch contract details from Insphire."""
    response = requests.get(
        f"{INSPIRE_BASE_URL}/contracts?api_key={session_id}&$filter=CONTNO eq '{contno}'&fields=ORDBYEMAIL,TOTAL,DELPCODE,CONTNO"
    )
    
    if response.status_code != 200:
        raise Exception("Contract details not found")
    
    contract_details = response.json()
    return contract_details[0] if isinstance(contract_details, list) else contract_details


def format_attributes(contract_items, contract_details, jobtype):
    """Format attributes based on the job type."""
    attributes = {}
    
    if jobtype == "Quote_Event_Created":
        for item in contract_items:
            attributes.update({
                'contract_number': item.get("CONTNO"),
                'orderby_email': contract_details.get("ORDBYEMAIL", ""),
                'item': item.get("ITEMNO"),
                'item_desc': item.get("ITEMDESC3", ""),
                'analysis_code': item.get("ANLCODE"),
                'rate_code': item.get("RATECODE"),
                'rate': item.get("RATE1"),
                'total_value': contract_details.get("TOTAL"),
                'quote_date': item.get("HIREDATE"),
                'hire_start_date': item.get("HIREDATE"),
                'est_hire_end': item.get("ESTRETD"),
                'depot': item.get("DEPOT"),
                'postcode': contract_details.get("DELPCODE", ""),
                'damage_waiver': item.get("INSURANCE")
            })
    
    elif jobtype == "Order":
        for item in contract_items:
            attributes.update({
                'contract_number': contract_details.get("CONTNO"),
                'orderby_email': contract_details.get("ORDBYEMAIL", ""),
                'item': item.get("ITEMNO"),
                'item_desc': item.get("ITEMDESC3", ""),
                'analysis_code': item.get("ANLCODE"),
                'rate_code': item.get("RATECODE"),
                'rate': item.get("RATE1"),
                'total_value': contract_details.get("TOTAL"),
                'quote_date': item.get("HIREDATE"),
                'hire_start_date': item.get("HIREDATE"),
                'est_hire_end': item.get("ESTRETD", ""),
                'depot': item.get("DEPOT"),
                'postcode': contract_details.get("DELPCODE", "")
            })
    
    return attributes


def send_event_to_moengage(customer_id: str, jobtype: str, attributes: dict):
    """Send event data to MoEngage."""
    event_payload = {
        "type": "event",
        "customer_id": customer_id,
        "actions": [{
            "action": jobtype,
            "attributes": attributes
        }]
    }
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {base64.b64encode(f'{MOENGAGE_USERNAME}:{MOENGAGE_PASSWORD}'.encode()).decode()}"
    }
    
    response = requests.post(MOENGAGE_EVENT_URL, headers=headers, json=event_payload)
    if response.status_code != 200:
        raise Exception(f"Failed to send event: {response.text}")



# from fastapi import FastAPI, HTTPException
# import requests

# app = FastAPI()

# INSPIRE_BASE_URL = "https://ihweb.afi-uplift.co.uk/insphire.corporate/api"

# # Function to authenticate and get session ID
# def get_session():
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

# # Function to fetch contract items and codes
# def fetch_contract_items_and_codes(contno: str, session_id: str):
#     url = f"{INSPIRE_BASE_URL}/contractitems?api_key={session_id}&$filter=CONTNO eq '{contno}'&fields=ITEMNO,RATECODE,RATE1,HIREDATE,ESTRETD,DEPOT,INSURANCE,CONTNO,ITEMDESC3"
#     response = requests.get(url)
#     if response.status_code != 200:
#         raise HTTPException(status_code=404, detail="Contract items not found")

#     contract_items = response.json()

#     for item in contract_items:
#         item_no = item.get("ITEMNO")
#         if item_no:  # Only fetch analysis code for valid items
#             analysis_code = fetch_analysis_code(item_no, session_id)
#             item['ANLCODE'] = analysis_code

#     return contract_items

# # Function to fetch analysis code
# def fetch_analysis_code(item_no: str, session_id: str):
#     url = f"{INSPIRE_BASE_URL}/stock/{item_no}?api_key={session_id}&fields=ANLCODE"
#     response = requests.get(url)
#     if response.status_code != 200:
#         return None  # Handle missing analysis code gracefully
#     return response.json().get("ANLCODE")

# # Function to fetch contract details
# def fetch_contract_details(contno: str, session_id: str):
#     url = f"{INSPIRE_BASE_URL}/contracts?api_key={session_id}&$filter=CONTNO eq '{contno}'&fields=ORDBYEMAIL,TOTAL,DELPCODE,CONTNO"
#     response = requests.get(url)
#     if response.status_code != 200:
#         raise HTTPException(status_code=404, detail="Contract details not found")

#     contract_details = response.json()
#     if not contract_details:
#         return None

#     return contract_details[0] if isinstance(contract_details, list) else contract_details

# @app.get("/fetch_contract_data/")
# async def fetch_contract_data(contno: str, jobtype: str):
#     session_id = get_session()  # Get session ID for API calls
#     contract_items = fetch_contract_items_and_codes(contno, session_id)
#     contract_details = fetch_contract_details(contno, session_id)

#     if not contract_details:
#         raise HTTPException(status_code=404, detail="No contract details found")

#     response_data = {}

#     if jobtype == "Quote_Event_Created":
#         for item in contract_items:
#             response_data.update({
#                 'contract_number': item.get("CONTNO"),
#                 'orderby_email': contract_details.get("ORDBYEMAIL", ""),
#                 'item': item.get("ITEMNO"),
#                 'item_desc': item.get("ITEMDESC3", ""),
#                 'analysis_code': item.get("ANLCODE"),
#                 'rate_code': item.get("RATECODE"),
#                 'rate': item.get("RATE1"),
#                 'total_value': contract_details.get("TOTAL"),
#                 'quote_date': item.get("HIREDATE"),
#                 'hire_start_date': item.get("HIREDATE"),
#                 'est_hire_end': item.get("ESTRETD"),
#                 'depot': item.get("DEPOT"),
#                 'postcode': contract_details.get("DELPCODE", ""),
#                 'damage_waiver': item.get("INSURANCE")
#             })

#     elif jobtype == "Order":
#         for item in contract_items:
#             response_data.update({
#                 'contract_number': contract_details.get("CONTNO"),
#                 'orderby_email': contract_details.get("ORDBYEMAIL", ""),
#                 'item': item.get("ITEMNO"),
#                 'item_desc': item.get("ITEMDESC3", ""),
#                 'analysis_code': item.get("ANLCODE"),
#                 'rate_code': item.get("RATECODE"),
#                 'rate': item.get("RATE1"),
#                 'total_value': contract_details.get("TOTAL"),
#                 'quote_date': item.get("HIREDATE"),
#                 'hire_start_date': item.get("HIREDATE"),
#                 'est_hire_end': contract_details.get("ESTRETD", ""),
#                 'depot': item.get("DEPOT"),
#                 'postcode': contract_details.get("DELPCODE", "")
#             })

#     elif jobtype == "LOST QUOTE":
#         for item in contract_items:
#             response_data.update({
#                 'contract_number': contract_details.get("CONTNO"),
#                 'orderby_email': contract_details.get("ORDBYEMAIL", ""),
#                 'item': item.get("ITEMNO"),
#                 'item_desc': item.get("ITEMDESC3", ""),
#                 'analysis_code': item.get("ANLCODE"),
#                 'rate_code': item.get("RATECODE"),
#                 'rate': item.get("RATE1"),
#                 'total_value': contract_details.get("TOTAL"),
#                 'quote_date': item.get("HIREDATE"),
#                 'hire_start_date': item.get("HIREDATE"),
#                 'est_hire_end': item.get("ESTRETD", ""),
#                 'depot': item.get("DEPOT"),
#                 'postcode': contract_details.get("DELPCODE", "")
#             })

#     elif jobtype == "Off Hire":
#         response_data.update({
#             'contract_number': contract_details.get("CONTNO"),
#             'off_hire_email': contract_details.get("ORDBYEMAIL", ""),
#             'item': contract_items[0].get("ITEMNO") if contract_items else "",
#             'analysis_code': contract_items[0].get("ANLCODE") if contract_items else "",
#             'date_of_off_hire': contract_details.get("DELPCODE", "")
#         })

#     elif jobtype == "Invoice":
#         for item in contract_items:
#             response_data.update({
#                 'contract_number': item.get("CONTNO"),
#                 'item': item.get("ITEMNO"),
#                 'email': contract_details.get("ORDBYEMAIL", ""),
#                 'analysis_code': item.get("ANLCODE")
#             })
#     else:
#         raise HTTPException(status_code=400, detail="Invalid job type")

#     return response_data





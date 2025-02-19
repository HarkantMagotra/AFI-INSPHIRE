from fastapi import APIRouter, HTTPException
import asyncio
import boto3, json, uuid
from datetime import datetime
import httpx
from app.middleware.middleware import get_session  # Import session function

router = APIRouter()

secrets_client = boto3.client("secretsmanager")
dynamodb = boto3.resource('dynamodb')

processed_table = dynamodb.Table("Test_Insphire_Processed")
error_log_table = dynamodb.Table("Test_Insphire_ErrorLog")

def log_error_to_dynamodb(email, error_message, payload):
    """Logs errors into the DynamoDB error log table."""
    try:
        error_log_table.put_item(Item={
            "id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "customerid": email,
            "error_msg": error_message,
            "payload": json.dumps(payload) if payload else "{}"
        })
    except Exception as db_error:
        print(f"Failed to log error to DynamoDB: {db_error}")




def get_secret(secret_name: str):
    """Fetch secrets from AWS Secrets Manager with error handling."""
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        if "SecretString" in response:
            return json.loads(response["SecretString"])
        elif "SecretBinary" in response:
            return json.loads(response["SecretBinary"])
    except Exception as e:
        error_message = f"Error fetching secret '{secret_name}': {str(e)}"
        log_error_to_dynamodb("get_secret", error_message, {})
        raise HTTPException(status_code=500, detail="Secrets retrieval failed")

secrets = get_secret("afi/crm/test")

if secrets:
    MOENGAGE_EVENT_API_URL_Test = secrets.get("MOENGAGE_EVENT_API_URL_Test", "")
    INSPIRE_BASE_URL = secrets.get("INSPIRE_BASE_URL", "")
    moe_token_test = secrets.get("moe_token_test", "")
else:
    error_message = "Secrets Manager values missing or invalid"
    log_error_to_dynamodb("get_secret", error_message, {})
    raise HTTPException(status_code=500, detail="Failed to load secrets")

token_moe = f"Basic {moe_token_test}"


async def fetch_contract_items_and_codes(contno: str, session_id: str):
    """Fetch contract items with analysis codes asynchronously."""
    contract_url = (
        f"{INSPIRE_BASE_URL}/contractitems?api_key={session_id}"
        f"&$filter=CONTNO eq '{contno}'&fields=ITEMNO,RATECODE,RATE1,"
        "HIREDATE,ESTRETD,DEPOT,INSURANCE,CONTNO,ITEMDESC3"
    )

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(contract_url)
            response.raise_for_status()  # Raise exception for 4xx and 5xx errors

            contract_items = response.json()
            if not contract_items:  # Handle empty response
                return []

            # Fetch analysis codes in parallel
            tasks = [
                fetch_analysis_code(item["ITEMNO"], session_id, client)
                for item in contract_items if "ITEMNO" in item
            ]
            analysis_codes = await asyncio.gather(*tasks, return_exceptions=True)

            for item, an_code in zip(contract_items, analysis_codes):
                item["ANLCODE"] = an_code if isinstance(an_code, str) else ""

            return contract_items
        except httpx.HTTPStatusError as e:
            error_message = f"HTTP Error: {e.response.status_code} while fetching contract items for {contno}"
            log_error_to_dynamodb("fetch_contract_items_and_codes", error_message, {"contno": contno})
            raise HTTPException(status_code=e.response.status_code, detail="Contract items not found")
        except httpx.RequestError as e:
            error_message = f"Request failed: {str(e)}"
            log_error_to_dynamodb("fetch_contract_items_and_codes", error_message, {"contno": contno})
            raise HTTPException(status_code=500, detail="Failed to fetch contract items")


async def fetch_analysis_code(item_no: str, session_id: str, client):
    """Fetch analysis code asynchronously with proper error handling."""
    url = f"{INSPIRE_BASE_URL}/stock/{item_no}?api_key={session_id}&fields=ANLCODE"
    try:
        response = await client.get(url)
        response.raise_for_status()
        return response.json().get("ANLCODE", "").strip()
    except httpx.HTTPStatusError as e:
        error_message = f"HTTP Error: {e.response.status_code} while fetching analysis code for {item_no}"
        log_error_to_dynamodb("fetch_analysis_code", error_message, {"item_no": item_no})
    except httpx.RequestError as e:
        error_message = f"Request failed: {str(e)}"
        log_error_to_dynamodb("fetch_analysis_code", error_message, {"item_no": item_no})
    return ""  # Default to empty string if any error occurs


async def fetch_contract_details(contno: str, session_id: str):
    """Fetch contract details asynchronously with error handling."""
    url = f"{INSPIRE_BASE_URL}/contracts?api_key={session_id}&$filter=CONTNO eq '{contno}'&fields=ORDBYEMAIL,TOTAL,DELPCODE,CONTNO"

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            contract_details = response.json()
            return contract_details[0] if isinstance(contract_details, list) else contract_details
        except httpx.HTTPStatusError as e:
            error_message = f"HTTP Error: {e.response.status_code} while fetching contract details for {contno}"
            log_error_to_dynamodb("fetch_contract_details", error_message, {"contno": contno})
            raise HTTPException(status_code=e.response.status_code, detail="Contract details not found")
        except httpx.RequestError as e:
            error_message = f"Request failed: {str(e)}"
            log_error_to_dynamodb("fetch_contract_details", error_message, {"contno": contno})
            raise HTTPException(status_code=500, detail="Failed to fetch contract details")



async def send_to_moengage(customer_id: str, event: str, attributes: dict): 
    """Send event data to MoEngage and handle errors properly."""
    
    try:
        if not token_moe or not MOENGAGE_EVENT_API_URL_Test:
            raise ValueError("Missing MoEngage token or API URL.")

        payload = {
            "type": "event",
            "customer_id": customer_id,
            "actions": [
                {
                    "action": event,
                    "attributes": attributes
                }
            ]
        }

        headers = {
            "Authorization": token_moe,
            "Content-Type": "application/json"
        }

        print("Payload:", payload)
        print("MoEngage API URL:", MOENGAGE_EVENT_API_URL_Test)

        async with httpx.AsyncClient() as client:
            response = await client.post(MOENGAGE_EVENT_API_URL_Test, json=payload, headers=headers)
            response.raise_for_status()  # Raises HTTPStatusError for non-2xx responses
        print("ji")
        # Log successful event processing
        processed_table.put_item(Item={
            "customer_id": customer_id,
            "timestamp": datetime.now().isoformat(),
        })
        print("out")

    except (httpx.HTTPStatusError, httpx.RequestError) as e:
        error_message = f"MoEngage API error: {str(e)}"
        log_error_to_dynamodb(customer_id, error_message, payload)
        raise HTTPException(status_code=500, detail=error_message)  # Standardized error handling

    except Exception as e:
        error_message = f"Unexpected error in send_to_moengage: {str(e)}"
        log_error_to_dynamodb(customer_id, error_message, payload)
        raise HTTPException(status_code=500, detail=error_message)

@router.get("/fetch_contract_data/")
async def fetch_contract_data(contno: str, event: str):

    try:   
        session_id = get_session()

        contract_items, contract_details = await asyncio.gather(
            fetch_contract_items_and_codes(contno, session_id),
            fetch_contract_details(contno, session_id),
        )

        if not contract_details:
            raise HTTPException(status_code=404, detail="No contract details found")

        response_data = {}

        if event == "Quote_Event_Created":
            for item in contract_items:
                response_data.update({
                    'contract_number': item.get("CONTNO"),
                    'email': contract_details.get("ORDBYEMAIL", ""),
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

        elif event == "Order":
            for item in contract_items:
                response_data.update({
                    'contract_number': contract_details.get("CONTNO"),
                    'email': contract_details.get("ORDBYEMAIL", ""),
                    'item': item.get("ITEMNO"),
                    'item_desc': item.get("ITEMDESC3", ""),
                    'analysis_code': item.get("ANLCODE"),
                    'rate_code': item.get("RATECODE"),
                    'rate': item.get("RATE1"),
                    'total_value': contract_details.get("TOTAL"),
                    'quote_date': item.get("HIREDATE"),
                    'hire_start_date': item.get("HIREDATE"),
                    'est_hire_end': contract_details.get("ESTRETD", ""),
                    'depot': item.get("DEPOT"),
                    'postcode': contract_details.get("DELPCODE", "")
                })

        elif event == "LOST QUOTE":
            for item in contract_items:
                response_data.update({
                    'contract_number': contract_details.get("CONTNO"),
                    'email': contract_details.get("ORDBYEMAIL", ""),
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

        elif event == "Off Hire":
            response_data.update({
                'contract_number': contract_details.get("CONTNO"),
                'email': contract_details.get("ORDBYEMAIL", ""),
                'item': contract_items[0].get("ITEMNO") if contract_items else "",
                'analysis_code': contract_items[0].get("ANLCODE") if contract_items else "",
                'date_of_off_hire': contract_details.get("DELPCODE", "")
            })

        elif event == "Invoice":
            for item in contract_items:
                response_data.update({
                    'contract_number': item.get("CONTNO"),
                    'item': item.get("ITEMNO"),
                    'email': contract_details.get("ORDBYEMAIL", ""),
                    'analysis_code': item.get("ANLCODE")
                })
        else:
            raise HTTPException(status_code=400, detail="Invalid job type")

        print("response data")
        print(response_data)

        await send_to_moengage(response_data.get("email", ""), event, response_data)

        return response_data
    
    except Exception as e:
        log_error_to_dynamodb(response_data.get("email", ""), str(e), response_data)
        raise HTTPException(status_code=500, detail="Failed to fetch contract data")

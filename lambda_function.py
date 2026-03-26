import json
import logging
from datetime import datetime, timedelta
from utils import (
    execute_query,
    respond,
    send_report_generation_message
)
from customer_report_config import (
    handle_create_update_reporting_config,
    get_enabled_customer_master_ids
)

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """Main Lambda handler function for both customer and reporting config operations.
    
    Handles:
    1. API Gateway requests (GET/POST/PUT for /report endpoints)
    2. EventBridge scheduler events (batch report generation)
    3. SQS trigger events (for debugging and monitoring)
    """
    # ============================================================
    # CHECKPOINT 1: Initial Event Detection
    # ============================================================
    logger.info("=" * 80)
    logger.info("CHECKPOINT 1: Lambda Handler Started")
    logger.info(f"Event Type Detection - Event Keys: {list(event.keys())}")
    logger.info(f"Full Event Structure: {json.dumps(event, default=str, indent=2)}")
    logger.info("=" * 80)
    
    # ============================================================
    # CHECKPOINT 2: SQS Event Detection
    # ============================================================
    if "Records" in event:
        # Check if this is an SQS event
        first_record = event["Records"][0] if event.get("Records") else {}
        event_source = first_record.get("eventSource", "")
        
        logger.info("=" * 80)
        logger.info("CHECKPOINT 2: SQS Event Detected")
        logger.info(f"Number of Records: {len(event.get('Records', []))}")
        logger.info(f"Event Source: {event_source}")
        logger.info(f"First Record Keys: {list(first_record.keys())}")
        
        if event_source == "aws:sqs":
            logger.info("✓ Confirmed: This is an SQS trigger event")
            return handle_sqs_event(event, context)
        else:
            logger.warning(f"⚠️ Records found but eventSource is '{event_source}' (not aws:sqs)")
            logger.info("Treating as non-SQS event, continuing with normal flow...")
    
    # ============================================================
    # CHECKPOINT 3: EventBridge Event Detection
    # ============================================================
    if "source" in event and event.get("source") == "aws.events":
        logger.info("=" * 80)
        logger.info("CHECKPOINT 3: EventBridge Scheduled Event Detected")
        logger.info(f"Event Detail: {json.dumps(event.get('detail', {}), default=str)}")
        logger.info("=" * 80)
        return handle_scheduled_report_generation(event, context)
    
    # ============================================================
    # CHECKPOINT 4: Empty Event Detection
    # ============================================================
    # Check if event is None, empty dict, or has no keys
    is_empty = (event is None) or (isinstance(event, dict) and len(event) == 0) or (not bool(event))
    
    if is_empty:
        logger.warning("=" * 80)
        logger.warning("⚠️ EMPTY EVENT DETECTED - No event data provided")
        logger.warning("This usually happens when testing with an empty test event.")
        logger.warning("Please provide a proper test event structure:")
        logger.warning("  - For API Gateway: Include 'httpMethod' and 'path'")
        logger.warning("  - For SQS: Include 'Records' array")
        logger.warning("  - For EventBridge: Include 'source' = 'aws.events'")
        logger.warning("=" * 80)
        return {
            "statusCode": 400,
            "body": json.dumps({
                "error": "Empty event received",
                "message": "Please provide a valid test event. See CloudWatch logs for examples.",
                "examples": {
                    "api_gateway": {
                        "httpMethod": "POST",
                        "path": "/report/generate",
                        "body": "{\"customerMasterId\":\"Test_002\",\"period\":{\"startDate\":\"2025-10-01\",\"endDate\":\"2025-10-31\"},\"requestId\":\"TEST-001\"}"
                    },
                    "sqs": {
                        "Records": [{
                            "eventSource": "aws:sqs",
                            "body": "{\"customerMasterId\":\"Test_002\",\"period\":{\"startDate\":\"2025-10-01\",\"endDate\":\"2025-10-31\"},\"requestId\":\"TEST-001\"}"
                        }]
                    }
                }
            }, default=str)
        }
    
    # ============================================================
    # CHECKPOINT 5: API Gateway Event Detection
    # ============================================================
    method = event.get("httpMethod", "").upper()
    path = event.get("path", "").lower()
    
    logger.info("=" * 80)
    logger.info("CHECKPOINT 5: API Gateway Event Detected")
    logger.info(f"HTTP Method: {method}")
    logger.info(f"Path: {path}")
    logger.info(f"Query Parameters: {event.get('queryStringParameters', {})}")
    logger.info(f"Headers: {json.dumps(event.get('headers', {}), default=str)}")
    logger.info("=" * 80)
    
    # Handle CORS preflight requests
    if method == "OPTIONS":
        logger.info("Handling OPTIONS preflight request")
        return handle_cors_preflight()
    
    # Route based on path - handle both /report and /ghg/report
    if path == "/report" or path == "/ghg/report":
        # Handle reporting configuration requests
        if method == "GET":
            logger.info("CHECKPOINT 6: Routing to GET customers handler")
            return handle_get_customers(event)
        elif method == "PUT":
            logger.info("CHECKPOINT 6: Routing to PUT config handler")
            return handle_create_update_reporting_config(event)
        elif method == "POST":
            # Check if this is a report generation request or config update
            # For now, POST on /report is for config, /report/generate is for generation
            logger.info("CHECKPOINT 6: Routing to POST config handler")
            return handle_create_update_reporting_config(event)
        else:
            logger.warning(f"Method {method} not allowed for path {path}")
            return respond(405, {"error": f"Method {method} Not Allowed for reporting configuration"})
    elif path == "/report/generate" or path == "/ghg/report/generate":
        # Handle report generation requests
        if method == "POST":
            logger.info("CHECKPOINT 6: Routing to POST report generation handler")
            # Check if this is a batch generation request
            body_str = event.get("body", "{}")
            if isinstance(body_str, str):
                body = json.loads(body_str)
            else:
                body = body_str
            
            if body.get("batch", False) or body.get("generateAll", False):
                logger.info("CHECKPOINT 7: Batch generation detected")
                return handle_batch_report_generation(event)
            else:
                logger.info("CHECKPOINT 7: Single report generation detected")
                return handle_generate_report(event)
        else:
            logger.warning(f"Method {method} not allowed for path {path}")
            return respond(405, {"error": f"Method {method} Not Allowed for report generation"})
    else:
        logger.warning(f"Path {path} not found")
        return respond(404, {"error": f"Path {path} not found"})


def handle_sqs_event(event, context):
    """Handle SQS trigger events with comprehensive debugging.
    
    This function processes SQS messages that trigger this Lambda.
    Useful for debugging SQS integration and monitoring message flow.
    
    Expected SQS Event Structure:
    {
        "Records": [
            {
                "messageId": "19dd0b57-b21e-4ac1-bd88-01b068e83eaf",
                "receiptHandle": "AQEBzWwaftRI0KuVm4tP...",
                "body": "{\"customerMasterId\":\"...\",\"period\":{...}}",
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1523232000000",
                    "SenderId": "AIDAIENQZJOLO23YVJ4VO",
                    "ApproximateFirstReceiveTimestamp": "1523232000001"
                },
                "messageAttributes": {},
                "md5OfBody": "7b270e59b47ff90a553787216d55d91d",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-1:...",
                "awsRegion": "us-east-1"
            }
        ]
    }
    """
    logger.info("=" * 80)
    logger.info("CHECKPOINT 7: SQS Event Handler Started")
    logger.info(f"Total Records: {len(event.get('Records', []))}")
    logger.info("=" * 80)
    
    records = event.get('Records', [])
    processed_count = 0
    failed_count = 0
    results = []
    
    for idx, record in enumerate(records):
        logger.info("=" * 80)
        logger.info(f"CHECKPOINT 8: Processing Record {idx + 1}/{len(records)}")
        logger.info(f"Message ID: {record.get('messageId', 'N/A')}")
        logger.info(f"Event Source: {record.get('eventSource', 'N/A')}")
        logger.info(f"Event Source ARN: {record.get('eventSourceARN', 'N/A')}")
        logger.info(f"AWS Region: {record.get('awsRegion', 'N/A')}")
        logger.info("=" * 80)
        
        # ============================================================
        # CHECKPOINT 9: Message Attributes
        # ============================================================
        attributes = record.get('attributes', {})
        logger.info("CHECKPOINT 9: Message Attributes")
        logger.info(f"  - ApproximateReceiveCount: {attributes.get('ApproximateReceiveCount', 'N/A')}")
        logger.info(f"  - SentTimestamp: {attributes.get('SentTimestamp', 'N/A')}")
        logger.info(f"  - SenderId: {attributes.get('SenderId', 'N/A')}")
        logger.info(f"  - ApproximateFirstReceiveTimestamp: {attributes.get('ApproximateFirstReceiveTimestamp', 'N/A')}")
        
        # Check for poison messages (received too many times)
        receive_count = int(attributes.get('ApproximateReceiveCount', 0))
        if receive_count > 3:
            logger.error(f"⚠️ POISON MESSAGE DETECTED: Message received {receive_count} times!")
            logger.error("This message should be moved to DLQ or investigated")
        
        # ============================================================
        # CHECKPOINT 10: Parse Message Body
        # ============================================================
        logger.info("CHECKPOINT 10: Parsing Message Body")
        body_str = record.get('body', '{}')
        logger.info(f"Raw Body (first 200 chars): {body_str[:200]}")
        logger.info(f"Body MD5: {record.get('md5OfBody', 'N/A')}")
        
        try:
            if isinstance(body_str, str):
                message_body = json.loads(body_str)
            else:
                message_body = body_str
            
            logger.info(f"✓ Message Body Parsed Successfully")
            logger.info(f"Parsed Body: {json.dumps(message_body, default=str, indent=2)}")
            
        except json.JSONDecodeError as e:
            logger.error(f"✗ FAILED to parse message body as JSON: {e}")
            logger.error(f"Raw body: {body_str}")
            failed_count += 1
            results.append({
                "messageId": record.get('messageId'),
                "status": "failed",
                "error": f"JSON decode error: {str(e)}"
            })
            continue
        
        # ============================================================
        # CHECKPOINT 11: Validate Message Structure
        # ============================================================
        logger.info("CHECKPOINT 11: Validating Message Structure")
        required_fields = ['customerMasterId', 'period', 'requestId']
        missing_fields = [field for field in required_fields if field not in message_body]
        
        if missing_fields:
            logger.error(f"✗ Missing required fields: {missing_fields}")
            logger.error(f"Available fields: {list(message_body.keys())}")
            failed_count += 1
            results.append({
                "messageId": record.get('messageId'),
                "status": "failed",
                "error": f"Missing required fields: {missing_fields}",
                "available_fields": list(message_body.keys())
            })
            continue
        
        logger.info("✓ All required fields present")
        logger.info(f"  - customerMasterId: {message_body.get('customerMasterId')}")
        logger.info(f"  - requestId: {message_body.get('requestId')}")
        logger.info(f"  - period: {message_body.get('period')}")
        logger.info(f"  - fileType: {message_body.get('fileType', 'not specified')}")
        
        # ============================================================
        # CHECKPOINT 12: Process Message (Log Only - No Actual Processing)
        # ============================================================
        logger.info("CHECKPOINT 12: Message Validation Complete")
        logger.info("NOTE: This Lambda (cer-dev-report-lambda) is for API/EventBridge only.")
        logger.info("SQS messages should be processed by cer-dev-report-worker-lambda")
        logger.info("Messages sent to 'report_generator_worker' queue will AUTO-TRIGGER the worker Lambda")
        logger.info("This handler is for debugging/monitoring purposes only.")
        
        processed_count += 1
        results.append({
            "messageId": record.get('messageId'),
            "status": "validated",
            "customerMasterId": message_body.get('customerMasterId'),
            "requestId": message_body.get('requestId'),
            "message": "Message validated successfully (not processed - worker Lambda should handle)"
        })
        
        logger.info(f"✓ Record {idx + 1} processed successfully")
        logger.info("=" * 80)
    
    # ============================================================
    # CHECKPOINT 13: Final Summary
    # ============================================================
    logger.info("=" * 80)
    logger.info("CHECKPOINT 13: SQS Event Processing Summary")
    logger.info(f"Total Records: {len(records)}")
    logger.info(f"Processed: {processed_count}")
    logger.info(f"Failed: {failed_count}")
    logger.info("=" * 80)
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "status": "completed",
            "total_records": len(records),
            "processed": processed_count,
            "failed": failed_count,
            "results": results
        }, default=str)
    }


def handle_cors_preflight():
    """Handle CORS preflight OPTIONS requests"""
    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,X-Requested-With",
            "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Max-Age": "86400",
            "Content-Type": "application/json"
        },
        "body": json.dumps({"message": "OK"})
    }


def handle_get_customers(event):
    """Handle GET request to fetch customer list"""
    try:
        # Get query parameters
        query_params = event.get("queryStringParameters") or {}

        # Extract parameters
        status = query_params.get("status")
        master_client_name = query_params.get("master_client_name")
        master_client_id = query_params.get("master_client_id")

        # Handle different status scenarios
        if status == 'true':
            # Get all active customers
            query, params = build_active_customers_query(master_client_name, master_client_id)
        elif status == 'false':
            # Get inactive customers with inner join to customer_reporting_config
            query, params = build_inactive_customers_query(master_client_name, master_client_id)
        else:
            # Default behavior - get all customers
            query, params = build_customer_query(master_client_name, master_client_id)

        # Execute query
        customers = execute_query(query, params)

        return respond(200, {
            "customers": customers
        })

    except Exception as e:
        logger.error(f"Error fetching customers: {e}")
        return respond(500, {"error": "Internal server error"})


def build_customer_query(master_client_name=None, master_client_id=None):
    """Build the SQL query for fetching customer data with reporting config"""
    base_query = """
        SELECT DISTINCT ON (ch.master_client_id)
            ch.master_client_name,
            ch.master_client_id,
            TO_CHAR(ch.update_datetime, 'MM/DD/YYYY HH24:MI:SS') as update_datetime,
            COALESCE(crc.is_reporting_enabled, true) as is_reporting_enabled,
        FROM emission.customer_hierarchy ch
        LEFT JOIN emission.customer_reporting_config crc ON ch.master_client_id = crc.master_client_id
    """

    where_conditions = []
    params = []

    # Add filtering conditions
    if master_client_name:
        where_conditions.append("ch.master_client_name ILIKE %s")
        params.append(f"%{master_client_name}%")

    if master_client_id:
        where_conditions.append("ch.master_client_id ILIKE %s")
        params.append(f"%{master_client_id}%")

    # Add WHERE clause if conditions exist
    if where_conditions:
        base_query += " WHERE " + " AND ".join(where_conditions)

    # Add ordering only
    base_query += " ORDER BY ch.master_client_id, ch.update_datetime DESC"

    return base_query, params


def build_active_customers_query(master_client_name=None, master_client_id=None):
    """Build the SQL query for fetching active customer data with reporting config"""
    base_query = """
        SELECT DISTINCT ON (ch.master_client_id)
            ch.master_client_name,
            ch.master_client_id,
            TO_CHAR(ch.update_datetime, 'MM/DD/YYYY HH24:MI:SS') as update_datetime
        FROM emission.customer_hierarchy ch
        LEFT JOIN emission.customer_reporting_config crc ON ch.master_client_id = crc.master_client_id
    """

    where_conditions = ["COALESCE(crc.is_reporting_enabled, true) = true"]
    params = []

    # Add filtering conditions
    if master_client_name:
        where_conditions.append("ch.master_client_name ILIKE %s")
        params.append(f"%{master_client_name}%")

    if master_client_id:
        where_conditions.append("ch.master_client_id ILIKE %s")
        params.append(f"%{master_client_id}%")

    # Add WHERE clause
    base_query += " WHERE " + " AND ".join(where_conditions)
    base_query += " ORDER BY ch.master_client_id, ch.update_datetime DESC"

    return base_query, params


def build_inactive_customers_query(master_client_name=None, master_client_id=None):
    """Build the SQL query for fetching inactive customer data with reporting config"""
    base_query = """
        SELECT DISTINCT ON (ch.master_client_id)
            ch.master_client_name,
            ch.master_client_id
        FROM emission.customer_hierarchy ch
        LEFT JOIN emission.customer_reporting_config crc ON ch.master_client_id = crc.master_client_id
    """

    where_conditions = ["COALESCE(crc.is_reporting_enabled, true) = false"]
    params = []

    # Add filtering conditions
    if master_client_name:
        where_conditions.append("ch.master_client_name ILIKE %s")
        params.append(f"%{master_client_name}%")

    if master_client_id:
        where_conditions.append("ch.master_client_id ILIKE %s")
        params.append(f"%{master_client_id}%")

    # Add WHERE clause
    base_query += " WHERE " + " AND ".join(where_conditions)
    base_query += " ORDER BY ch.master_client_id, ch.update_datetime DESC"

    return base_query, params


def handle_generate_report(event):
    """Handle POST request to generate report by sending message to SQS queue.
    
    This function validates the request, prepares the message, and sends it to
    the SQS queue. The worker Lambda will automatically process the message
    and generate the Excel report.
    
    Expected request body:
    {
        "customerMasterId": "Test_002",
        "period": {
            "startDate": "2025-10-01",
            "endDate": "2025-10-31"
        },
        "requestId": "Test_002",
        "fileType": "xlsx",  // optional
        "customerFileName": "Customer Name",  // optional
        "businessPartnerId": "BP001"  // optional
    }
    
    Returns:
        HTTP 202 Accepted: Request accepted and queued
        HTTP 400 Bad Request: Validation error
        HTTP 500 Internal Server Error: SQS or other error
    """
    try:
        # Parse request body
        body_str = event.get("body", "{}")
        if isinstance(body_str, str):
            body = json.loads(body_str)
        else:
            body = body_str
        
        logger.info(f"Report generation request: {json.dumps(body, default=str)}")
        
        # Validate required fields
        required_fields = ["customerMasterId", "period", "requestId"]
        missing_fields = [field for field in required_fields if field not in body]
        
        if missing_fields:
            return respond(400, {
                "error": f"Missing required fields: {', '.join(missing_fields)}",
                "required_fields": required_fields,
                "received_fields": list(body.keys())
            })
        
        # Validate period structure
        period = body.get("period")
        if not isinstance(period, dict):
            return respond(400, {
                "error": "period must be an object",
                "received_type": type(period).__name__
            })
        
        period_required = ["startDate", "endDate"]
        missing_period_fields = [field for field in period_required if field not in period]
        
        if missing_period_fields:
            return respond(400, {
                "error": f"period must contain: {', '.join(missing_period_fields)}",
                "required_period_fields": period_required,
                "received_period_fields": list(period.keys())
            })
        
        # Validate date format (basic validation)
        try:
            from datetime import datetime
            datetime.strptime(period["startDate"], '%Y-%m-%d')
            datetime.strptime(period["endDate"], '%Y-%m-%d')
        except ValueError as e:
            return respond(400, {
                "error": f"Invalid date format. Expected YYYY-MM-DD: {str(e)}",
                "startDate": period.get("startDate"),
                "endDate": period.get("endDate")
            })
        
        # Prepare message data for SQS
        # This message will be sent to 'report_generator_worker' queue
        # The worker Lambda (cer-dev-report-worker-lambda) has Event Source Mapping configured
        # When this message arrives in the queue, it will AUTO-TRIGGER the worker Lambda
        # 
        # Required fields for worker Lambda:
        #   - customerMasterId: Customer identifier (REQUIRED)
        #   - period.startDate: Start date in YYYY-MM-DD format (REQUIRED)
        #   - period.endDate: End date in YYYY-MM-DD format (REQUIRED)
        #   - requestId: Unique request identifier (REQUIRED)
        # Optional fields:
        #   - fileType: "xlsx", "xls", or "csv" (default: "csv")
        #   - customerFileName: Customer name for filename
        #   - businessPartnerId: Business partner ID
        #
        # ========================================================================
        # TEST DATA CONFIGURATION
        # ========================================================================
        # Set USE_TEST_DATA = True to use hardcoded test data
        # Set USE_TEST_DATA = False to use actual request body data
        USE_TEST_DATA = True  # Using test data from below
        
        # Single test data entry for testing
        test_data = {
            "customerMasterId": "Casetest_001",
            "period": {
                "startDate": "2025-10-01",
                "endDate": "2025-10-31"
            },
            "requestId": "REQ-001-OCT2025",
            "fileType": "xlsx",
            "businessPartnerId": "19940710",
            "customerFileName": "CIBC_mastercard_Carbone"
        }
        
        # Choose data source: test data or request body
        if USE_TEST_DATA:
            # Use hardcoded test data
            message_data = test_data
            logger.info(f"Using TEST DATA: customerMasterId={message_data.get('customerMasterId')}, requestId={message_data.get('requestId')}")
        else:
            # Use dynamic message data from request body
            message_data = {
                "customerMasterId": body["customerMasterId"],
                "period": {
                    "startDate": period["startDate"],
                    "endDate": period["endDate"]
                },
                "requestId": body["requestId"],
                "fileType": body.get("fileType", "xlsx")  # Default to xlsx
            }
            
            # Add optional fields if provided
            if "customerFileName" in body:
                message_data["customerFileName"] = body["customerFileName"]
            if "businessPartnerId" in body:
                message_data["businessPartnerId"] = body["businessPartnerId"]
            
            logger.info(f"Using REQUEST BODY data: customerMasterId={message_data.get('customerMasterId')}, requestId={message_data.get('requestId')}")
        
        logger.info(f"Prepared message data: {json.dumps(message_data, default=str)}")
        
        # Send message to SQS
        try:
            result = send_report_generation_message(message_data)
            
            logger.info(f"Report generation request sent successfully: {result}")
            
            return respond(202, {
                "message": "Report generation request accepted",
                "requestId": result["requestId"],
                "messageId": result["messageId"],
                "status": "queued",
                "queueUrl": result.get("queueUrl", "N/A")
            })
            
        except ValueError as e:
            # Validation error from utils.send_report_generation_message
            logger.error(f"Validation error in SQS helper: {e}")
            return respond(400, {"error": str(e)})
        except Exception as e:
            # SQS or other error
            logger.error(f"Error sending message to SQS: {e}", exc_info=True)
            return respond(500, {
                "error": "Failed to queue report generation request",
                "details": str(e)
            })
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in request body: {e}")
        return respond(400, {
            "error": "Invalid JSON in request body",
            "details": str(e)
        })
    except Exception as e:
        logger.error(f"Unexpected error in handle_generate_report: {e}", exc_info=True)
        return respond(500, {
            "error": "Internal server error",
            "details": str(e)
        })


def handle_scheduled_report_generation(event, context):
    """Handle EventBridge scheduled event for batch report generation.
    
    This function is triggered by EventBridge scheduler on a set date and time.
    It retrieves all enabled customers and generates reports for each.
    
    Expected event structure (from EventBridge):
    {
        "source": "aws.events",
        "detail-type": "Scheduled Event",
        "detail": {
            "period": {
                "startDate": "2025-01-01",  // Optional, defaults to previous month
                "endDate": "2025-01-31"     // Optional, defaults to previous month
            }
        }
    }
    """
    try:
        logger.info("Processing scheduled report generation event")
        
        # Extract period from event detail, or default to previous month
        detail = event.get("detail", {})
        period = detail.get("period", {})
        
        # Default to previous month if not specified
        if not period.get("startDate") or not period.get("endDate"):
            today = datetime.now()
            first_day_current_month = today.replace(day=1)
            last_day_previous_month = first_day_current_month - timedelta(days=1)
            first_day_previous_month = last_day_previous_month.replace(day=1)
            
            period_start_date = first_day_previous_month.strftime('%Y-%m-%d')
            period_end_date = last_day_previous_month.strftime('%Y-%m-%d')
        else:
            period_start_date = period["startDate"]
            period_end_date = period["endDate"]
        
        logger.info(f"Processing reports for period: {period_start_date} to {period_end_date}")
        
        # Generate batch reports
        result = generate_batch_reports(period_start_date, period_end_date, request_id=f"SCHEDULED-{datetime.now().strftime('%Y%m%d-%H%M%S')}")
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Scheduled report generation completed",
                "result": result
            })
        }
        
    except Exception as e:
        logger.error(f"Error in scheduled report generation: {e}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": "Internal server error",
                "details": str(e)
            })
        }


def handle_batch_report_generation(event):
    """Handle batch report generation request from API.
    
    This function can be called via API to generate reports for all enabled customers.
    
    Expected request body:
    {
        "batch": true,
        "period": {
            "startDate": "2025-01-01",
            "endDate": "2025-01-31"
        },
        "requestId": "BATCH-2025-01"  // Optional
    }
    """
    try:
        # Parse request body
        body_str = event.get("body", "{}")
        if isinstance(body_str, str):
            body = json.loads(body_str)
        else:
            body = body_str
        
        logger.info(f"Batch report generation request: {json.dumps(body, default=str)}")
        
        # Validate period
        period = body.get("period", {})
        if not period.get("startDate") or not period.get("endDate"):
            return respond(400, {
                "error": "period with startDate and endDate is required for batch generation"
            })
        
        period_start_date = period["startDate"]
        period_end_date = period["endDate"]
        
        # Validate date format
        try:
            datetime.strptime(period_start_date, '%Y-%m-%d')
            datetime.strptime(period_end_date, '%Y-%m-%d')
        except ValueError as e:
            return respond(400, {
                "error": f"Invalid date format. Expected YYYY-MM-DD: {str(e)}"
            })
        
        request_id = body.get("requestId", f"BATCH-{datetime.now().strftime('%Y%m%d-%H%M%S')}")
        
        # Generate batch reports
        result = generate_batch_reports(period_start_date, period_end_date, request_id)
        
        return respond(202, {
            "message": "Batch report generation request accepted",
            "requestId": request_id,
            "result": result
        })
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in request body: {e}")
        return respond(400, {
            "error": "Invalid JSON in request body",
            "details": str(e)
        })
    except Exception as e:
        logger.error(f"Unexpected error in handle_batch_report_generation: {e}", exc_info=True)
        return respond(500, {
            "error": "Internal server error",
            "details": str(e)
        })


def generate_batch_reports(period_start_date, period_end_date, request_id=None):
    """
    Generate reports for all enabled customers.
    
    This is the main business logic function that:
    1. Retrieves list of all enabled customer master IDs
    2. Loops through each customer and sends SQS message
    
    Args:
        period_start_date (str): Start date in YYYY-MM-DD format
        period_end_date (str): End date in YYYY-MM-DD format
        request_id (str, optional): Request identifier for tracking
    
    Returns:
        dict: Summary of batch operation:
            - total_customers (int): Number of customers processed
            - successful (int): Number of successful SQS messages
            - failed (int): Number of failed SQS messages
    """
    try:
        logger.info(f"Starting batch report generation for period {period_start_date} to {period_end_date}")
        
        # Step 1: Get all enabled customer master IDs
        enabled_customers = get_enabled_customer_master_ids()
        total_customers = len(enabled_customers)
        
        if total_customers == 0:
            logger.warning("No enabled customers found for report generation")
            return {
                "total_customers": 0,
                "successful": 0,
                "failed": 0,
                "message": "No enabled customers found"
            }
        
        logger.info(f"Found {total_customers} enabled customers. Starting to send SQS messages...")
        
        # Step 2: Loop through customers and send SQS messages
        successful = 0
        failed = 0
        failed_customers = []
        
        for customer in enabled_customers:
            master_client_id = customer.get("master_client_id")
            master_client_name = customer.get("master_client_name", master_client_id)
            
            if not master_client_id:
                logger.warning(f"Skipping customer with no master_client_id: {customer}")
                failed += 1
                continue
            
            try:
                # Prepare message for this customer
                message_data = {
                    "customerMasterId": master_client_id,
                    "period": {
                        "startDate": period_start_date,
                        "endDate": period_end_date
                    },
                    "requestId": request_id or f"{master_client_id}-{datetime.now().strftime('%Y%m%d')}",
                    "fileType": "xlsx",  # Default to Excel
                    "customerFileName": master_client_name
                }
                
                # Send message to SQS
                result = send_report_generation_message(message_data)
                successful += 1
                logger.info(f"Sent SQS message for customer {master_client_id}: {result.get('messageId')}")
                
            except Exception as e:
                failed += 1
                failed_customers.append({
                    "master_client_id": master_client_id,
                    "error": str(e)
                })
                logger.error(f"Failed to send SQS message for customer {master_client_id}: {e}")
        
        logger.info(f"Batch SQS message sending completed: {successful} successful, {failed} failed")
        
        result = {
            "total_customers": total_customers,
            "successful": successful,
            "failed": failed,
            "period": {
                "startDate": period_start_date,
                "endDate": period_end_date
            }
        }
        
        if failed_customers:
            result["failed_customers"] = failed_customers[:10]  # Limit to first 10 for response size
        
        logger.info(f"Batch report generation completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Error in generate_batch_reports: {e}", exc_info=True)
        raise

import json
import os
import logging
import pg8000
from datetime import datetime
from decimal import Decimal
import time
import boto3
from botocore.exceptions import ClientError
from functools import lru_cache
import ssl

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# CORS configuration
CORS_ORIGIN = os.environ.get("CORS_ORIGIN", "*")

# Secrets Manager configuration
SECRET_ARN = os.environ.get('app_user_secret_arn',
                            'arn:aws:secretsmanager:us-east-1:209479303537:secret:cer/ingest/dev/db-cluster/emission/credentials-CJwkgd')

# Secret manager client
sm_client = None

# SQS Configuration
# Queue Name: report_generator_worker
# Queue ARN: arn:aws:sqs:us-east-1:666128536964:report_generator_worker
# Worker Lambda: cer-dev-report-worker-lambda (processes messages from this queue)
SQS_QUEUE_NAME = os.environ.get('SQS_QUEUE_NAME', 'report_generator_worker')
AWS_REGION = os.environ.get('REGION_NAME', 'us-east-1')

# SQS client (singleton pattern for performance)
sqs_client = None


def _get_sm_client():
    global sm_client
    if sm_client is None:
        session = boto3.session.Session()
        sm_client = session.client(service_name='secretsmanager', region_name='us-east-1')
    return sm_client


def _get_sqs_client():
    """Get or create SQS client (singleton pattern for performance).
    
    Returns:
        boto3.client: SQS client instance
    """
    global sqs_client
    if sqs_client is None:
        sqs_client = boto3.client('sqs', region_name=AWS_REGION)
        logger.info(f"SQS client initialized for region: {AWS_REGION}")
    return sqs_client


@lru_cache(maxsize=1)
def get_secret():
    """Retrieve database credentials from AWS Secrets Manager"""
    try:
        get_secret_value_response = _get_sm_client().get_secret_value(
            SecretId=SECRET_ARN
        )
    except ClientError as e:
        logger.error(f"Error retrieving secret: {e}")
        raise
    else:
        if 'SecretString' in get_secret_value_response:
            secret = json.loads(get_secret_value_response['SecretString'])
            return secret
        else:
            logger.error("Secret not found in SecretString")
            raise ValueError("Secret not found in SecretString")


# def get_db_config():
#     """Get database configuration from Secrets Manager"""
#     try:
#         secret = get_secret()

#         return {
#             'host': secret['DB_HOST'],
#             'database': secret['DB_NAME'],
#             'user': secret['DB_USER'],
#             'password': secret['password'],
#             'port': int(secret.get('DB_PORT', 5432)),
#             'timeout': 10,  # pg8000 uses 'timeout' instead of 'connect_timeout'
#             'application_name': 'lambda-report'  # Optional: add application name
#         }
#     except Exception as e:
#         logger.error(f"Error getting database config from Secrets Manager: {e}")
#         raise

def get_db_config():
    """Get database configuration from Secrets Manager"""
    # try:
    secret = get_secret()
    # 🔐 STRICT SSL CONTEXT (RDS CA)
    # ssl_context = ssl.create_default_context(
    #     cafile="/opt/certs/global-bundle.pem"
    # )

    # ssl_context.check_hostname = True
    # ssl_context.verify_mode = ssl.CERT_REQUIRED

    ssl_context = ssl.create_default_context()

    return {
        'host': secret['DB_HOST'],
        'database': secret['DB_NAME'],
        'user': secret['DB_USER'],
        'password': secret['password'],
        'port': int(secret.get('DB_PORT', 5432)),
        'timeout': 10,
        'application_name': 'lambda-report',
        'ssl_context': ssl_context
    }
 
    # except Exception as e:
    #     logger.error(f"Error getting database config from Secrets Manager: {e}")
    #     raise


def get_db_connection(max_retries=3, retry_delay=2):
    """Create and return a database connection with retry logic"""
    db_config = get_db_config()

    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting database connection (attempt {attempt + 1}/{max_retries})")
            connection = pg8000.connect(**db_config)

            # Set statement timeout after connection (60 seconds for large result sets)
            cursor = connection.cursor()
            cursor.execute("SET statement_timeout = 60000")
            cursor.close()

            # Set work_mem for better performance on large operations
            cursor = connection.cursor()
            cursor.execute("SET work_mem = '256MB'")
            cursor.close()

            # Set other performance optimizations
            cursor = connection.cursor()
            cursor.execute("SET enable_seqscan = off")  # Prefer index scans
            cursor.execute("SET random_page_cost = 1.1")  # Optimize for SSD
            cursor.execute("SET effective_cache_size = '1GB'")  # Assume 1GB cache
            cursor.close()

            # Test the connection
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()

            logger.info("Database connection successful")
            return connection

        except pg8000.OperationalError as e:
            logger.error(f"Database connection attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("All database connection attempts failed")
                raise
        except Exception as e:
            logger.error(f"Unexpected database connection error: {e}")
            raise


def execute_query(query, params=None, fetch_all=True):
    """Execute a query and return results"""
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        logger.info(f"Executing query with {len(params) if params else 0} parameters")

        # Add query optimization hints for large result sets
        if not params or len(params) == 0:
            cursor.execute("SET enable_nestloop = off")  # Disable nested loops for large joins
            cursor.execute("SET enable_hashjoin = on")  # Enable hash joins
            cursor.execute("SET enable_mergejoin = on")  # Enable merge joins

        cursor.execute(query, params or [])

        if fetch_all:
            results = cursor.fetchall()
            # Get column names
            column_names = [desc[0] for desc in cursor.description]

            # Convert results to list of dictionaries
            data = []
            for row in results:
                item = dict(zip(column_names, row))
                # Convert datetime and decimal objects to appropriate types
                for key, value in item.items():
                    if isinstance(value, datetime):
                        # Format as MM/DD/YYYY HH24:MI:SS for consistency
                        item[key] = value.strftime('%m/%d/%Y %H:%M:%S')
                    elif isinstance(value, Decimal):
                        item[key] = int(value) if value % 1 == 0 else float(value)
                data.append(item)
            return data
        else:
            result = cursor.fetchone()
            if result:
                # Get column names
                column_names = [desc[0] for desc in cursor.description]
                item = dict(zip(column_names, result))
                # Convert datetime and decimal objects to appropriate types
                for key, value in item.items():
                    if isinstance(value, datetime):
                        # Format as MM/DD/YYYY HH24:MI:SS for consistency
                        item[key] = value.strftime('%m/%d/%Y %H:%M:%S')
                    elif isinstance(value, Decimal):
                        item[key] = int(value) if value % 1 == 0 else float(value)
                return item
            return None

    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise
    finally:
        if connection:
            connection.close()


def execute_count_query(query, params=None):
    """Execute a count query and return total count"""
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        cursor.execute(query, params or [])
        result = cursor.fetchone()

        count = result[0] if result else 0
        logger.info(f"Total count: {count}")
        return count

    except Exception as e:
        logger.error(f"Error executing count query: {e}")
        raise
    finally:
        if connection:
            connection.close()


def execute_update_query(query, params=None):
    """Execute an update/insert query and return affected rows"""
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        cursor.execute(query, params or [])
        connection.commit()

        rows_affected = cursor.rowcount
        logger.info(f"Rows affected: {rows_affected}")
        return rows_affected

    except Exception as e:
        logger.error(f"Error executing update query: {e}")
        if connection:
            connection.rollback()
        raise
    finally:
        if connection:
            connection.close()


def execute_batch_query(query, params_list):
    """Execute a batch query with multiple parameter sets and return results"""
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        logger.info(f"Executing batch query with {len(params_list)} parameter sets")

        # Execute batch query
        cursor.executemany(query, params_list)

        # Fetch all results
        results = cursor.fetchall()

        # Get column names
        column_names = [desc[0] for desc in cursor.description]

        # Convert results to list of dictionaries
        data = []
        for row in results:
            item = dict(zip(column_names, row))
            # Convert datetime and decimal objects to appropriate types
            for key, value in item.items():
                if isinstance(value, datetime):
                    # Format as MM/DD/YYYY HH24:MI:SS for consistency
                    item[key] = value.strftime('%m/%d/%Y %H:%M:%S')
                elif isinstance(value, Decimal):
                    item[key] = int(value) if value % 1 == 0 else float(value)
            data.append(item)

        logger.info(f"Batch query completed successfully. Returned {len(data)} results")
        return data

    except Exception as e:
        logger.error(f"Error executing batch query: {e}")
        if connection:
            connection.rollback()
        raise
    finally:
        if connection:
            connection.close()


def execute_batch_update_query(query, params_list):
    """Execute a batch update/insert query and return affected rows"""
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        logger.info(f"Executing batch update query with {len(params_list)} parameter sets")

        # Execute batch query
        cursor.executemany(query, params_list)
        connection.commit()

        rows_affected = cursor.rowcount
        logger.info(f"Total rows affected in batch: {rows_affected}")
        return rows_affected

    except Exception as e:
        logger.error(f"Error executing batch update query: {e}")
        if connection:
            connection.rollback()
        raise
    finally:
        if connection:
            connection.close()


def respond(status_code, body_dict):
    """Create HTTP response with proper headers"""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,X-Requested-With",
            "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Max-Age": "86400"
        },
        "body": json.dumps(body_dict, default=str)
    }


def validate_required_fields(data, required_fields):
    """Validate that required fields are present in the data"""
    missing_fields = []
    for field in required_fields:
        if field not in data or data[field] is None:
            missing_fields.append(field)

    if missing_fields:
        raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")

    return True


def list_available_queues():
    """List all available SQS queues (for debugging).
    
    Returns:
        list: List of queue URLs
    """
    try:
        sqs = _get_sqs_client()
        response = sqs.list_queues()
        queue_urls = response.get('QueueUrls', [])
        logger.info(f"Found {len(queue_urls)} available SQS queues")
        for url in queue_urls:
            logger.info(f"  - {url}")
        return queue_urls
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"Failed to list queues: {error_code} - {error_message}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error listing queues: {e}")
        return []


def get_sqs_queue_url():
    """Get SQS queue URL.
    
    Returns:
        str: Queue URL
        
    Raises:
        ClientError: If queue doesn't exist or access is denied
    """
    try:
        sqs = _get_sqs_client()
        logger.info(f"Attempting to get queue URL for: '{SQS_QUEUE_NAME}' in region: {AWS_REGION}")
        response = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)
        queue_url = response['QueueUrl']
        logger.info(f"✓ Queue URL retrieved successfully: {queue_url}")
        return queue_url
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        
        logger.error("=" * 80)
        logger.error("❌ FAILED TO GET QUEUE URL")
        logger.error(f"  - Error Code: {error_code}")
        logger.error(f"  - Error Message: {error_message}")
        logger.error(f"  - Queue Name: {SQS_QUEUE_NAME}")
        logger.error(f"  - Region: {AWS_REGION}")
        logger.error(f"  - Environment Variable SQS_QUEUE_NAME: {os.environ.get('SQS_QUEUE_NAME', 'NOT SET')}")
        logger.error("=" * 80)
        
        # Try to list available queues for debugging
        logger.info("Attempting to list available queues for debugging...")
        try:
            available_queues = list_available_queues()
            if available_queues:
                logger.info("=" * 80)
                logger.info("📋 AVAILABLE QUEUES (for reference):")
                for queue_url in available_queues:
                    # Extract queue name from URL
                    queue_name = queue_url.split('/')[-1]
                    logger.info(f"  - Queue Name: {queue_name}")
                    logger.info(f"  - Queue URL: {queue_url}")
                logger.info("=" * 80)
                logger.info("💡 TROUBLESHOOTING:")
                logger.info("  1. Check if queue name matches exactly (case-sensitive)")
                logger.info("  2. Verify queue exists in the same region (us-east-1)")
                logger.info("  3. Check Lambda IAM role has 'sqs:GetQueueUrl' permission")
                logger.info("  4. If queue name is different, set SQS_QUEUE_NAME environment variable")
            else:
                logger.warning("⚠️ No queues found or no permission to list queues")
                logger.info("💡 TROUBLESHOOTING:")
                logger.info("  1. Queue may not exist - create it in AWS SQS Console")
                logger.info("  2. Lambda IAM role may not have SQS permissions")
                logger.info("  3. Check if queue is in the correct region (us-east-1)")
        except Exception as list_error:
            logger.warning(f"Could not list queues for debugging: {list_error}")
        
        logger.error("=" * 80)
        raise


def send_report_generation_message(message_data):
    """
    Send report generation request to SQS queue with debug checkpoints.
    
    AUTO-TRIGGER FLOW:
    ==================
    1. This function sends a message to 'report_generator_worker' SQS queue
    2. The message contains valid customer data (customerMasterId, period, requestId, etc.)
    3. Worker Lambda (cer-dev-report-worker-lambda) has Event Source Mapping configured
    4. When message arrives in queue, AWS Lambda service AUTO-TRIGGERS the worker Lambda
    5. Worker Lambda processes the message and generates the report
    
    IMPORTANT: Message body must contain valid data (not empty) with required fields:
    - customerMasterId (REQUIRED)
    - period.startDate (REQUIRED)
    - period.endDate (REQUIRED)
    - requestId (REQUIRED)
    
    This function validates the message data, converts it to JSON, and sends it
    to the SQS queue. The worker Lambda will automatically process this message.
    
    Args:
        message_data (dict): Message data with required fields:
            - customerMasterId (str): Customer identifier
            - period (dict): {
                - startDate (str): Start date in YYYY-MM-DD format
                - endDate (str): End date in YYYY-MM-DD format
              }
            - requestId (str): Unique request identifier
            - fileType (str, optional): "xlsx", "xls", or "csv" (default: "csv")
            - customerFileName (str, optional): Customer name for filename
            - businessPartnerId (str, optional): Business partner ID
    
    Returns:
        dict: Response containing:
            - messageId (str): SQS message ID
            - requestId (str): Request ID from message
            - status (str): "sent"
            - queueUrl (str): Queue URL where message was sent
    
    Raises:
        ValueError: If required fields are missing or invalid
        ClientError: If SQS operation fails
    """
    # ============================================================
    # CHECKPOINT A: Function Entry
    # ============================================================
    logger.info("=" * 80)
    logger.info("CHECKPOINT A: send_report_generation_message() - Entry")
    logger.info(f"Input message_data: {json.dumps(message_data, default=str, indent=2)}")
    logger.info("=" * 80)
    
    # ============================================================
    # CHECKPOINT B: Environment Variables
    # ============================================================
    logger.info("CHECKPOINT B: Environment Configuration")
    logger.info(f"  - SQS_QUEUE_NAME: {SQS_QUEUE_NAME}")
    logger.info(f"  - AWS_REGION: {AWS_REGION}")
    logger.info(f"  - Queue Name from ENV: {os.environ.get('SQS_QUEUE_NAME', 'NOT SET')}")
    
    # ============================================================
    # CHECKPOINT C: Validation
    # ============================================================
    logger.info("CHECKPOINT C: Validating Message Data")
    try:
        validate_required_fields(message_data, ['customerMasterId', 'period', 'requestId'])
        logger.info("✓ Required fields validation passed")
    except ValueError as e:
        logger.error(f"✗ Validation failed: {e}")
        raise
    
    period = message_data.get('period')
    if not isinstance(period, dict):
        error_msg = "period must be a dictionary"
        logger.error(f"✗ {error_msg}")
        raise ValueError(error_msg)
    
    if 'startDate' not in period or 'endDate' not in period:
        error_msg = "period must contain startDate and endDate"
        logger.error(f"✗ {error_msg}")
        raise ValueError(error_msg)
    
    logger.info("✓ Period validation passed")
    
    # Validate date format (basic check)
    try:
        datetime.strptime(period['startDate'], '%Y-%m-%d')
        datetime.strptime(period['endDate'], '%Y-%m-%d')
        logger.info("✓ Date format validation passed")
    except ValueError as e:
        error_msg = f"Invalid date format. Expected YYYY-MM-DD: {e}"
        logger.error(f"✗ {error_msg}")
        raise ValueError(error_msg)
    
    # ============================================================
    # CHECKPOINT D: SQS Client Initialization
    # ============================================================
    logger.info("CHECKPOINT D: Initializing SQS Client")
    try:
        sqs = _get_sqs_client()
        logger.info("✓ SQS client initialized successfully")
    except Exception as e:
        logger.error(f"✗ Failed to initialize SQS client: {e}")
        raise
    
    # ============================================================
    # CHECKPOINT E: Get Queue URL
    # ============================================================
    logger.info("CHECKPOINT E: Getting Queue URL")
    logger.info(f"Attempting to get queue URL for: {SQS_QUEUE_NAME}")
    logger.info(f"Region: {AWS_REGION}")
    logger.info(f"Environment Variable SQS_QUEUE_NAME: {os.environ.get('SQS_QUEUE_NAME', 'NOT SET - using default')}")
    try:
        queue_url = get_sqs_queue_url()
        logger.info(f"✓ Queue URL retrieved: {queue_url}")
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_msg = e.response.get('Error', {}).get('Message', str(e))
        logger.error("=" * 80)
        logger.error("✗ Failed to get queue URL")
        logger.error(f"  - Error Code: {error_code}")
        logger.error(f"  - Error Message: {error_msg}")
        logger.error(f"  - Queue Name: {SQS_QUEUE_NAME}")
        logger.error(f"  - Region: {AWS_REGION}")
        logger.error("=" * 80)
        logger.error("🔧 ACTION REQUIRED:")
        logger.error("  1. Verify queue 'report_generator_worker' exists in SQS Console")
        logger.error("  2. Check Lambda IAM role has 'sqs:GetQueueUrl' permission")
        logger.error("  3. Verify queue is in the same region (us-east-1)")
        logger.error("  4. If queue name is different, set SQS_QUEUE_NAME environment variable")
        logger.error("=" * 80)
        raise
    except Exception as e:
        logger.error(f"✗ Unexpected error getting queue URL: {e}")
        raise
    
    # ============================================================
    # CHECKPOINT F: Prepare Message
    # ============================================================
    logger.info("CHECKPOINT F: Preparing SQS Message")
    message_body = json.dumps(message_data)
    logger.info(f"Message body (JSON): {message_body}")
    logger.info(f"Message body length: {len(message_body)} bytes")
    
    # Prepare message attributes (optional metadata for filtering/monitoring)
    message_attributes = {
        'customerMasterId': {
            'StringValue': message_data['customerMasterId'],
            'DataType': 'String'
        },
        'requestId': {
            'StringValue': message_data['requestId'],
            'DataType': 'String'
        },
        'fileType': {
            'StringValue': message_data.get('fileType', 'csv'),
            'DataType': 'String'
        }
    }
    logger.info(f"Message attributes: {json.dumps(message_attributes, default=str)}")
    
    # ============================================================
    # CHECKPOINT G: Send Message
    # ============================================================
    logger.info("CHECKPOINT G: Sending Message to SQS")
    logger.info(f"  - Queue URL: {queue_url}")
    logger.info(f"  - Message Body: {message_body}")
    
    try:
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body,
            MessageAttributes=message_attributes
        )
        
        message_id = response.get('MessageId')
        md5_of_body = response.get('MD5OfMessageBody')
        
        logger.info("=" * 80)
        logger.info("CHECKPOINT H: Message Sent Successfully")
        logger.info(f"  - Message ID: {message_id}")
        logger.info(f"  - MD5 of Body: {md5_of_body}")
        logger.info(f"  - Response: {json.dumps(response, default=str)}")
        logger.info("=" * 80)
        
        # ============================================================
        # CHECKPOINT J: Verify Message in Queue
        # ============================================================
        logger.info("CHECKPOINT J: Verifying Message in Queue")
        try:
            # Get queue attributes to verify message count
            queue_attributes = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
            )
            visible_messages = queue_attributes['Attributes'].get('ApproximateNumberOfMessages', '0')
            not_visible_messages = queue_attributes['Attributes'].get('ApproximateNumberOfMessagesNotVisible', '0')
            
            logger.info(f"  - Queue: {SQS_QUEUE_NAME}")
            logger.info(f"  - Queue URL: {queue_url}")
            logger.info(f"  - Approximate Visible Messages: {visible_messages}")
            logger.info(f"  - Approximate Not Visible Messages: {not_visible_messages}")
            logger.info(f"  - Total Messages in Queue: {int(visible_messages) + int(not_visible_messages)}")
            
            if int(visible_messages) > 0 or int(not_visible_messages) > 0:
                logger.info("✓ Message confirmed in queue - Worker Lambda should process it")
            else:
                logger.warning("⚠️ Queue shows 0 messages - Message may have been processed immediately")
                logger.warning("   This is normal if worker Lambda processed the message very quickly")
            
        except Exception as e:
            logger.warning(f"Could not verify queue attributes: {e}")
            logger.info("Message was sent successfully, but queue verification failed")
        
        result = {
            "messageId": message_id,
            "requestId": message_data['requestId'],
            "status": "sent",
            "queueUrl": queue_url,
            "md5OfBody": md5_of_body,
            "queueName": SQS_QUEUE_NAME
        }
        
        logger.info(f"send_report_generation_message completed: {result}")
        logger.info("=" * 80)
        logger.info("MESSAGE SEND VERIFICATION:")
        logger.info("  ✓ Message sent to queue: report_generator_worker")
        logger.info("  ✓ Message contains valid data (not empty)")
        logger.info("  ✓ Worker Lambda will auto-trigger when message arrives")
        logger.info("")
        logger.info("SQS METRICS EXPLANATION:")
        logger.info("  - NumberOfMessagesSent: Should increase when you send messages")
        logger.info("  - NumberOfEmptyReceives: High count is NORMAL if:")
        logger.info("    1. Queue is empty (no messages sent yet)")
        logger.info("    2. Messages are processed quickly (worker Lambda processes immediately)")
        logger.info("    3. Worker Lambda polls queue continuously (Event Source Mapping active)")
        logger.info("  - NumberOfMessagesReceived: Should increase when worker processes messages")
        logger.info("=" * 80)
        return result
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_msg = e.response.get('Error', {}).get('Message', str(e))
        logger.error("=" * 80)
        logger.error("CHECKPOINT I: Message Send Failed")
        logger.error(f"  - Error Code: {error_code}")
        logger.error(f"  - Error Message: {error_msg}")
        logger.error(f"  - Queue URL: {queue_url}")
        logger.error("=" * 80)
        raise
    except Exception as e:
        logger.error(f"✗ Unexpected error sending message: {e}", exc_info=True)
        raise

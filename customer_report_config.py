import json
import logging
from utils import (
    execute_query, 
    execute_update_query, 
    execute_count_query, 
    execute_batch_query,
    execute_batch_update_query,
    respond, 
    validate_required_fields
)

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handle_create_update_reporting_config(event):
    """Handle POST/PUT request to create or update reporting configuration for multiple clients"""
    try:
        # Parse request body
        body = json.loads(event.get("body", "{}"))
        
        # Get configs array
        configs = body.get("configs", [])
        if not isinstance(configs, list) or len(configs) == 0:
            return respond(400, {"error": "configs must be a non-empty array"})
        
        # Validate each configuration
        required_fields = ["master_client_id", "is_reporting_enabled", "action_taken_by"]
        validated_configs = []
        
        for i, config in enumerate(configs):
            try:
                validate_required_fields(config, required_fields)
                
                # Validate boolean value
                if not isinstance(config["is_reporting_enabled"], bool):
                    return respond(400, {"error": f"is_reporting_enabled must be a boolean value in config {i+1}"})
                
                # Clean master_client_id (remove extra spaces and validate)
                config["master_client_id"] = config["master_client_id"].strip()
                if not config["master_client_id"]:
                    return respond(400, {"error": f"master_client_id cannot be empty in config {i+1}"})
                
                validated_configs.append(config)
            except ValueError as e:
                return respond(400, {"error": f"Validation error in config {i+1}: {str(e)}"})
        
        # Process configurations in batch
        try:
            results = batch_upsert_reporting_configs(validated_configs)
            
            # Count successes and failures
            success_count = sum(1 for result in results if result["status"] == "success")
            error_count = len(results) - success_count
            
            # Prepare response
            response_data = {
                "results": results,
                "summary": {
                    "total": len(validated_configs),
                    "successful": success_count,
                    "failed": error_count
                }
            }
            
            # Return appropriate status code based on results
            if error_count == 0:
                return respond(200, response_data)
            elif success_count == 0:
                return respond(500, response_data)
            else:
                return respond(207, response_data)  # Multi-Status response
                
        except Exception as e:
            logger.error(f"Error in batch processing: {e}")
            return respond(500, {"error": "Internal server error during batch processing"})
        
    except json.JSONDecodeError:
        return respond(400, {"error": "Invalid JSON in request body"})
    except Exception as e:
        logger.error(f"Error creating/updating reporting configuration: {e}")
        return respond(500, {"error": "Internal server error"})

def batch_upsert_reporting_configs(configs):
    """Batch upsert reporting configurations using PostgreSQL ON CONFLICT"""
    if not configs:
        return []
    
    # Remove duplicates based on master_client_id (keep the last occurrence)
    unique_configs = {}
    for config in configs:
        unique_configs[config["master_client_id"]] = config
    
    # Prepare data for batch insert
    values_list = []
    for config in unique_configs.values():
        values_list.append((
            config["master_client_id"],
            config["is_reporting_enabled"],
            config["action_taken_by"]
        ))
    
    logger.info(f"Processing {len(unique_configs)} unique configurations (removed {len(configs) - len(unique_configs)} duplicates)")
    
    # Build the batch upsert query
    query = """
        INSERT INTO emission.customer_reporting_config 
        (master_client_id, is_reporting_enabled, action_taken_by, create_datetime, update_datetime)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (master_client_id) 
        DO UPDATE SET 
            is_reporting_enabled = EXCLUDED.is_reporting_enabled,
            action_taken_by = EXCLUDED.action_taken_by,
            update_datetime = CURRENT_TIMESTAMP
        RETURNING master_client_id, is_reporting_enabled, action_taken_by, create_datetime, update_datetime
    """
    
    try:
        # Execute batch upsert
        rows_affected = execute_batch_update_query(query, values_list)
        
        # Since batch upsert succeeded, return static success response
        # No need to fetch from database - we know the operation succeeded
        results = []
        for config in configs:
            # Get the cleaned master_client_id (same as used in unique_configs key)
            cleaned_master_client_id = config["master_client_id"]  # Already cleaned during validation
            results.append({
                "master_client_id": config["master_client_id"],  # Keep original for tracking
                "status": "success",
                "action": "upserted",
                "config": {
                    "master_client_id": cleaned_master_client_id,  # Use cleaned version
                    "is_reporting_enabled": unique_configs[cleaned_master_client_id]["is_reporting_enabled"],
                    "action_taken_by": unique_configs[cleaned_master_client_id]["action_taken_by"],
                    "create_datetime": "CURRENT_TIMESTAMP",  # Will be set by database
                    "update_datetime": "CURRENT_TIMESTAMP"   # Will be set by database
                }
            })
        
        return results
        
    except Exception as e:
        logger.error(f"Error in batch upsert: {e}")
        # Fallback to individual processing if batch fails
        logger.info("Falling back to individual processing")
        return fallback_individual_processing(configs)

def fallback_individual_processing(configs):
    """Fallback method for individual processing if batch fails"""
    results = []
    
    for config in configs:
        try:
            master_client_id = config["master_client_id"]
            is_reporting_enabled = config["is_reporting_enabled"]
            action_taken_by = config["action_taken_by"]
            
            # Check if configuration exists
            existing_config = get_reporting_config_by_master_client_id(master_client_id)
            
            if existing_config:
                # Update existing configuration
                success = update_reporting_config(master_client_id, is_reporting_enabled, action_taken_by)
                action = "updated"
            else:
                # Create new configuration
                success = create_reporting_config(master_client_id, is_reporting_enabled, action_taken_by)
                action = "created"
            
            if success:
                # Get the updated/created configuration
                updated_config = get_reporting_config_by_master_client_id(master_client_id)
                results.append({
                    "master_client_id": master_client_id,
                    "status": "success",
                    "action": action,
                    "config": updated_config
                })
            else:
                results.append({
                    "master_client_id": master_client_id,
                    "status": "error",
                    "message": f"Failed to {action} configuration"
                })
                
        except Exception as e:
            logger.error(f"Error processing config for master_client_id {config.get('master_client_id', 'unknown')}: {e}")
            results.append({
                "master_client_id": config.get("master_client_id", "unknown"),
                "status": "error",
                "message": str(e)
            })
    
    return results

def get_reporting_config_by_master_client_id(master_client_id):
    """Get reporting configuration by master_client_id"""
    query = """
        SELECT 
            master_client_id,
            is_reporting_enabled,
            action_taken_by,
            create_datetime,
            update_datetime
        FROM emission.customer_reporting_config
        WHERE master_client_id = %s
    """
    
    try:
        result = execute_query(query, [master_client_id], fetch_all=False)
        return result
    except Exception as e:
        logger.error(f"Error getting reporting config: {e}")
        raise

def get_all_reporting_configs(limit=100, offset=0):
    """Get all reporting configurations with pagination"""
    # Get configurations
    query = """
        SELECT 
            master_client_id,
            is_reporting_enabled,
            action_taken_by,
            create_datetime,
            update_datetime
        FROM emission.customer_reporting_config
        ORDER BY update_datetime DESC, create_datetime DESC
        LIMIT %s OFFSET %s
    """
    
    # Get total count
    count_query = "SELECT COUNT(*) FROM emission.customer_reporting_config"
    
    try:
        configs = execute_query(query, [limit, offset])
        total_count = execute_count_query(count_query)
        return configs, total_count
    except Exception as e:
        logger.error(f"Error getting all reporting configs: {e}")
        raise

def create_reporting_config(master_client_id, is_reporting_enabled, action_taken_by):
    """Create new reporting configuration"""
    query = """
        INSERT INTO emission.customer_reporting_config 
        (master_client_id, is_reporting_enabled, action_taken_by, create_datetime, update_datetime)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    """
    
    try:
        rows_affected = execute_update_query(query, [master_client_id, is_reporting_enabled, action_taken_by])
        return rows_affected > 0
    except Exception as e:
        logger.error(f"Error creating reporting config: {e}")
        raise

def update_reporting_config(master_client_id, is_reporting_enabled, action_taken_by):
    """Update existing reporting configuration"""
    query = """
        UPDATE emission.customer_reporting_config 
        SET is_reporting_enabled = %s, 
            action_taken_by = %s, 
            update_datetime = CURRENT_TIMESTAMP
        WHERE TRIM(master_client_id) = %s
    """
    
    try:
        rows_affected = execute_update_query(query, [is_reporting_enabled, action_taken_by, master_client_id])
        return rows_affected > 0
    except Exception as e:
        logger.error(f"Error updating reporting config: {e}")
        raise

def get_enabled_customer_master_ids():
    """Get all unique customer master IDs that have reporting enabled.
    
    This function retrieves all distinct customer master IDs from customers
    that have is_reporting_enabled = true (or default true if not set).
    
    Returns:
        list: List of dictionaries with master_client_id and master_client_name
        Example: [{"master_client_id": "CLIENT001", "master_client_name": "Client Name"}, ...]
    
    Raises:
        Exception: If database query fails
    """
    query = """
        SELECT DISTINCT 
            ch.master_client_id,
            ch.master_client_name
        FROM emission.customer_hierarchy ch
        LEFT JOIN emission.customer_reporting_config crc ON ch.master_client_id = crc.master_client_id
        WHERE COALESCE(crc.is_reporting_enabled, true) = true
          AND ch.master_client_id IS NOT NULL
          AND TRIM(ch.master_client_id) != ''
        ORDER BY ch.master_client_id
    """
    
    try:
        customers = execute_query(query, [])
        logger.info(f"Found {len(customers)} enabled customers for report generation")
        return customers
    except Exception as e:
        logger.error(f"Error getting enabled customer master IDs: {e}")
        raise

def get_customers_with_reporting_status(limit=100, offset=0, **filters):
    """Get customers with their reporting configuration status"""
    base_query = """
        SELECT 
            ch.customer_hierarchy_id,
            ch.client_id,
            ch.client_name,
            ch.client_office_code,
            ch.company_code,
            ch.customer_company_name,
            ch.data_source,
            ch.industry,
            ch.industry_code_text,
            ch.customer_number,
            ch.master_client_name,
            ch.mc_division,
            ch.mc_group,
            ch.mc_office,
            ch.sales_district_code,
            ch.sales_group,
            ch.sales_office,
            ch.industry_client_id,
            ch.industry_code_txt_client_id,
            ch.master_client_id,
            ch.account_id,
            ch.client_district_code,
            ch.client_group,
            ch.account_start_date,
            ch.create_datetime,
            ch.update_datetime,
            COALESCE(ch.status, 'ACTIVE') as status,
            COALESCE(crc.is_reporting_enabled, true) as is_reporting_enabled,
            crc.action_taken_by,
            crc.create_datetime as reporting_config_create_datetime,
            crc.update_datetime as reporting_config_update_datetime
        FROM emission.customer_hierarchy ch
        LEFT JOIN emission.customer_reporting_config crc ON ch.master_client_id = crc.master_client_id
        WHERE 1=1
    """
    
    params = []
    
    # Add filters
    if filters.get('client_id'):
        base_query += " AND ch.client_id = %s"
        params.append(filters['client_id'])
    
    if filters.get('company_code'):
        base_query += " AND ch.company_code = %s"
        params.append(filters['company_code'])
    
    if filters.get('industry'):
        base_query += " AND ch.industry ILIKE %s"
        params.append(f"%{filters['industry']}%")
    
    if filters.get('sales_office'):
        base_query += " AND ch.sales_office ILIKE %s"
        params.append(f"%{filters['sales_office']}%")
    
    # Add status filter
    if filters.get('status'):
        base_query += " AND COALESCE(ch.status, 'ACTIVE') = %s"
        params.append(filters['status'])
    
    # Add reporting enabled filter
    if filters.get('is_reporting_enabled') is not None:
        base_query += " AND COALESCE(crc.is_reporting_enabled, true) = %s"
        params.append(filters['is_reporting_enabled'])
    
    # Add ordering and pagination
    base_query += " ORDER BY ch.customer_hierarchy_id DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])
    
    try:
        customers = execute_query(base_query, params)
        return customers
    except Exception as e:
        logger.error(f"Error getting customers with reporting status: {e}")
        raise

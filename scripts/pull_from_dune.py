import os
import yaml
import json
import requests
from dotenv import load_dotenv
import sys
import codecs
import time

# Set the default encoding to UTF-8
sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path)

# Get API key from environment
API_KEY = os.getenv('DUNE_API_KEY')
if not API_KEY:
    print("Error: DUNE_API_KEY not found in .env file")
    sys.exit(1)

# Read the queries.yml file
queries_yml = os.path.join(os.path.dirname(__file__), '..', 'queries.yml')
with open(queries_yml, 'r', encoding='utf-8') as file:
    data = yaml.safe_load(file)

# Extract the query_ids from the data
query_ids = [str(id) for id in data['query_ids']]  # Ensure IDs are strings

# API configuration
BASE_URL = "https://api.dune.com/api/v1"
HEADERS = {
    "x-dune-api-key": API_KEY,
    "Content-Type": "application/json"
}

def get_execution_status(execution_id):
    """Get the status of a query execution"""
    url = f"https://api.dune.com/api/v1/execution/{execution_id}/status"
    headers = {
        "X-DUNE-API-KEY": API_KEY,
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        error_msg = f"Error getting status for execution {execution_id}: {e}"
        if hasattr(e, 'response') and e.response is not None:
            error_msg += f" - {e.response.status_code} - {e.response.text}"
        print(error_msg)
        return {"error": error_msg}

def execute_query(query_id):
    """Execute a query and return the execution ID"""
    url = f"https://api.dune.com/api/v1/query/{query_id}/execute"
    headers = {
        "X-DUNE-API-KEY": API_KEY,
        "Content-Type": "application/json"
    }
    
    try:
        print(f"Executing query ID: {query_id}")
        response = requests.post(
            url, 
            headers=headers, 
            json={"performance": "medium"}
        )
        response.raise_for_status()
        result = response.json()
        print(f"Execution started: {result}")
        return result
    except requests.exceptions.RequestException as e:
        error_msg = f"Error executing query {query_id}: {e}"
        if hasattr(e, 'response') and e.response is not None:
            error_msg += f" - {e.response.status_code} - {e.response.text}"
        print(error_msg)
        return {"error": error_msg}

def wait_for_completion(execution_id, max_attempts=30, delay=5):
    """Wait for query execution to complete"""
    print(f"Waiting for execution {execution_id} to complete...")
    
    for attempt in range(1, max_attempts + 1):
        status_data = get_execution_status(execution_id)
        
        if 'error' in status_data:
            return status_data
            
        state = status_data.get('state')
        print(f"Attempt {attempt}/{max_attempts} - Status: {state}")
        
        if state == 'QUERY_STATE_COMPLETED':
            return {"status": "completed"}
        elif state in ['QUERY_STATE_FAILED', 'QUERY_STATE_CANCELLED']:
            return {"error": f"Query execution {state.lower()}"}
            
        time.sleep(delay)
    
    return {"error": "Query execution timed out"}

def get_results(execution_id):
    """Get the results of a query execution"""
    url = f"https://api.dune.com/api/v1/execution/{execution_id}/results"
    headers = {
        "X-DUNE-API-KEY": API_KEY,
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        error_msg = f"Error getting results for execution {execution_id}: {e}"
        if hasattr(e, 'response') and e.response is not None:
            error_msg += f" - {e.response.status_code} - {e.response.text}"
        print(error_msg)
        return {"error": error_msg}

def save_results(query_id, results, results_dir):
    """Save query results to a JSON file"""
    if not results or 'error' in results:
        print(f"No valid results to save for query {query_id}")
        if 'error' in results:
            print(f"Error: {results['error']}")
        return
    
    # Create results directory if it doesn't exist
    os.makedirs(results_dir, exist_ok=True)
    
    # Create a safe filename
    results_filename = f"{query_id}_results.json"
    results_filepath = os.path.join(results_dir, results_filename)
    
    try:
        with open(results_filepath, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"Successfully saved results to: {results_filepath}")
    except Exception as e:
        print(f"Error saving results for query {query_id}: {e}")

def process_query(query_id, results_dir):
    """Process a single query by ID"""
    print(f"\n{'='*50}")
    print(f"Processing query ID: {query_id}")
    
    # Execute the query
    execution_result = execute_query(query_id)
    if 'error' in execution_result:
        print(f"Failed to execute query {query_id}: {execution_result['error']}")
        return
    
    execution_id = execution_result.get('execution_id')
    if not execution_id:
        print(f"No execution ID returned for query {query_id}")
        return
    
    # Wait for completion
    print(f"Waiting for query {query_id} to complete...")
    completion = wait_for_completion(execution_id)
    if 'error' in completion:
        print(f"Query execution failed: {completion['error']}")
        return
    
    # Get results
    print(f"Fetching results for query {query_id}...")
    results = get_results(execution_id)
    if 'error' in results:
        print(f"Failed to get results: {results['error']}")
        return results
    
    # Save results
    save_results(query_id, results, results_dir)
    return results

def main():
    # Setup directories
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    results_dir = os.path.join(base_dir, 'results')
    
    # Process each query
    for query_id in query_ids:
        process_query(query_id, results_dir)
    
    print("\nAll queries processed!")

if __name__ == "__main__":
    main()

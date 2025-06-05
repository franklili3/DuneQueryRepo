import os
import csv
import json
import requests
import time
import random
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables
load_dotenv()

# Configuration
API_KEY = os.getenv('DUNE_API_KEY')
if not API_KEY:
    raise ValueError("DUNE_API_KEY not found in .env file")

# File paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_CSV = os.path.join(BASE_DIR, 'exchange_usd_T_address_all.csv')
OUTPUT_CSV = os.path.join(BASE_DIR, 'exchange_usd_T_address_verified.csv')

# Request configuration
MAX_RETRIES = 3
INITIAL_DELAY = 5  # seconds
MAX_DELAY = 60  # seconds

def get_random_delay():
    """Get a random delay between requests to avoid rate limiting"""
    return random.uniform(1, 3)  # Random delay between 1-3 seconds

def safe_request(url, method='get', headers=None, json_data=None, max_retries=MAX_RETRIES):
    """Make a request with retries and exponential backoff"""
    for attempt in range(max_retries):
        try:
            if method.lower() == 'get':
                response = requests.get(url, headers=headers)
            else:
                response = requests.post(url, headers=headers, json=json_data)
            
            # If we hit rate limit, wait and retry
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 10))
                wait_time = min(retry_after, MAX_DELAY) + random.random() * 5  # Add some jitter
                print(f"Rate limited. Waiting {wait_time:.1f} seconds...")
                time.sleep(wait_time)
                continue
                
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                print(f"Request failed after {max_retries} attempts: {e}")
                return None
                
            wait_time = min(INITIAL_DELAY * (2 ** attempt) + random.random(), MAX_DELAY)
            print(f"Request failed (attempt {attempt + 1}/{max_retries}). Retrying in {wait_time:.1f}s...")
            time.sleep(wait_time)
    
    return None

def address_to_hex(address):
    """Convert Tron address to hex format"""
    # Tron addresses start with 'T' and are base58 encoded
    # For this example, we'll just return the address as is
    # as the Dune query expects a string parameter
    return address

def execute_dune_query(address_hex):
    """Execute Dune query with the given address parameter"""
    url = "https://api.dune.com/api/v1/query/5231060/execute"
    headers = {
        "X-DUNE-API-KEY": API_KEY,
        "Content-Type": "application/json"
    }
    
    # Set up query parameters - simplified format
    params = {
        "parameters": [
            {
                "key": "contract_address",
                "type": "text",
                "value": address_hex
            }
        ]
    }
    
    # Start query execution
    execution = safe_request(url, method='post', headers=headers, json_data=params)
    if not execution or 'execution_id' not in execution:
        print(f"Failed to start execution for address {address_hex}")
        return False
    
    execution_id = execution['execution_id']
    print(f"Started execution {execution_id} for address {address_hex}")
    
    # Wait for query to complete
    status_url = f"https://api.dune.com/api/v1/execution/{execution_id}/status"
    max_attempts = 30
    
    for attempt in range(max_attempts):
        time.sleep(get_random_delay())
        
        status = safe_request(status_url, headers=headers)
        if not status:
            print(f"Failed to get status for execution {execution_id}")
            return False
        
        state = status.get('state')
        print(f"Execution {execution_id} status: {state} (attempt {attempt + 1}/{max_attempts})")
        
        if state == 'QUERY_STATE_COMPLETED':
            break
        elif state in ['QUERY_STATE_FAILED', 'QUERY_STATE_CANCELLED']:
            print(f"Query execution {execution_id} failed or was cancelled")
            return False
            
        if attempt == max_attempts - 1:
            print(f"Query execution {execution_id} timed out")
            return False
    
    # Get results
    results_url = f"https://api.dune.com/api/v1/execution/{execution_id}/results"
    results = safe_request(results_url, headers=headers)
    
    if not results or 'result' not in results:
        print(f"No results returned for execution {execution_id}")
        return False
    
    # Check if there are any results
    has_results = bool(results.get('result', {}).get('rows'))
    print(f"Execution {execution_id} has results: {has_results}")
    return has_results

def process_csv():
    """Process the CSV file and add is_verified field"""
    # Read the input CSV
    with open(INPUT_CSV, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    # Process each row
    for row in tqdm(rows, desc="Processing addresses"):
        address = row['address']
        hex_address = address_to_hex(address)
        
        # Check if address is verified
        is_verified = execute_dune_query(hex_address)
        row['is_verified'] = 'yes' if is_verified else 'no'
    
    # Write to output CSV
    fieldnames = list(rows[0].keys())
    with open(OUTPUT_CSV, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"\nProcessing complete! Results saved to: {OUTPUT_CSV}")

if __name__ == "__main__":
    import time
    process_csv()

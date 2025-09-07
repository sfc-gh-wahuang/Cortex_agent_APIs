import requests
import json
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def delete_cortex_agent(token, agent_name, database, schema, account_url):
    """
    Delete a Cortex Data Agent
    
    Args:
        token: Bearer token for authentication
        agent_name: Name of the agent to delete
        database: Database name where the agent is stored
        schema: Schema name where the agent is stored
        account_url: Snowflake account URL
    
    Returns:
        requests.Response: The response object from the DELETE request
    """
    
    # API endpoint for deleting an agent
    api_endpoint = f"{account_url}/api/v2/databases/{database}/schemas/{schema}/agents/{agent_name}"
    
    # Request headers
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    
    # Send the DELETE request
    response = requests.delete(api_endpoint, headers=headers)
    return response

def delete_multiple_agents(token, agent_names, database, schema, account_url):
    """
    Delete multiple Cortex Data Agents
    
    Args:
        token: Bearer token for authentication
        agent_names: List of agent names to delete
        database: Database name where the agents are stored
        schema: Schema name where the agents are stored
        account_url: Snowflake account URL
    
    Returns:
        dict: Dictionary with agent names as keys and response status as values
    """
    
    results = {}
    
    for agent_name in agent_names:
        print(f"Deleting agent: {agent_name}")
        response = delete_cortex_agent(token, agent_name, database, schema, account_url)
        
        results[agent_name] = {
            'status_code': response.status_code,
            'success': response.status_code == 200,
            'response': response
        }
        
        if response.status_code == 200:
            print(f"✓ Successfully deleted agent: {agent_name}")
        else:
            print(f"✗ Failed to delete agent: {agent_name} (Status: {response.status_code})")
            try:
                error_data = response.json()
                print(f"  Error details: {error_data}")
            except:
                print(f"  Error text: {response.text}")
        
        print("-" * 40)
    
    return results

def confirm_deletion(agent_name):
    """
    Ask for user confirmation before deleting an agent
    
    Args:
        agent_name: Name of the agent to delete
    
    Returns:
        bool: True if user confirms deletion, False otherwise
    """
    while True:
        response = input(f"Are you sure you want to delete agent '{agent_name}'? (yes/no): ").lower().strip()
        if response in ['yes', 'y']:
            return True
        elif response in ['no', 'n']:
            return False
        else:
            print("Please enter 'yes' or 'no'")

# Example usage
if __name__ == "__main__":
    token = os.getenv("SNOWFLAKE_TOKEN")
    
    # Configuration - update these values for your environment
    database = "HOL2_DB"
    schema = "HOL2_SCHEMA"
    account_url = "https://eq06761.ap-southeast-2.snowflakecomputing.com"
    
    print("=== Cortex Agent Deletion Tool ===")
    print()
    
    # Example 1: Delete a single agent with confirmation
    agent_to_delete = "CUSTOM_AGENT"  # Replace with your agent name
    
    print(f"Agent to delete: {agent_to_delete}")
    print(f"Database: {database}")
    print(f"Schema: {schema}")
    print()
    
    # Uncomment the following lines to enable interactive confirmation
    # if confirm_deletion(agent_to_delete):
    #     response = delete_cortex_agent(
    #         token=token,
    #         agent_name=agent_to_delete,
    #         database=database,
    #         schema=schema,
    #         account_url=account_url
    #     )
    #     
    #     print(f"Status Code: {response.status_code}")
    #     
    #     if response.status_code == 200:
    #         print(f"✓ Successfully deleted agent: {agent_to_delete}")
    #     else:
    #         print(f"✗ Failed to delete agent: {agent_to_delete}")
    #         try:
    #             error_data = response.json()
    #             print(f"Error details: {json.dumps(error_data, indent=2)}")
    #         except:
    #             print(f"Error text: {response.text}")
    # else:
    #     print("Deletion cancelled.")
    
    # Example 2: Delete without confirmation (be careful!)
    print("WARNING: Deleting agent without confirmation!")
    response = delete_cortex_agent(
        token=token,
        agent_name=agent_to_delete,
        database=database,
        schema=schema,
        account_url=account_url
    )
    
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        print(f"✓ Successfully deleted agent: {agent_to_delete}")
    elif response.status_code == 404:
        print(f"Agent '{agent_to_delete}' not found (may already be deleted)")
    else:
        print(f"✗ Failed to delete agent: {agent_to_delete}")
        try:
            error_data = response.json()
            print(f"Error details: {json.dumps(error_data, indent=2)}")
        except:
            print(f"Error text: {response.text}")
    
    print("\n" + "="*60)
    print("=== Bulk Delete Example (Commented Out) ===")
    
    # Example 3: Delete multiple agents (commented out for safety)
    # agents_to_delete = [
    #     "TEST_AGENT_1",
    #     "TEST_AGENT_2", 
    #     "OLD_AGENT"
    # ]
    # 
    # print(f"Agents to delete: {agents_to_delete}")
    # print("WARNING: This will delete multiple agents!")
    # 
    # if input("Type 'DELETE ALL' to confirm bulk deletion: ") == "DELETE ALL":
    #     results = delete_multiple_agents(
    #         token=token,
    #         agent_names=agents_to_delete,
    #         database=database,
    #         schema=schema,
    #         account_url=account_url
    #     )
    #     
    #     print("\n=== Deletion Summary ===")
    #     for agent_name, result in results.items():
    #         status = "SUCCESS" if result['success'] else "FAILED"
    #         print(f"{agent_name}: {status} (Status: {result['status_code']})")
    # else:
    #     print("Bulk deletion cancelled.")
    
    print("\nDeletion process completed.")

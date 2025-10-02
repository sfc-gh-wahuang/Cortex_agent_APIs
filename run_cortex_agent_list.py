import requests
import json
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def list_cortex_agents(token, limit=None, offset=None):
    """
    List Snowflake Cortex agents
    
    Args:
        token: Bearer token for authentication
        limit: (Optional) Maximum number of agents to return
        offset: (Optional) Number of agents to skip
    """
    
    # API endpoint for listing agents
    api_endpoint = "https://eq06761.ap-southeast-2.snowflakecomputing.com/api/v2/databases/HOL2_DB/schemas/HOL2_SCHEMA/agents"
    
    # Request headers
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    # Query parameters
    params = {}
    if limit is not None:
        params['limit'] = limit
    if offset is not None:
        params['offset'] = offset
    
    # Send the GET request
    response = requests.get(api_endpoint, headers=headers, params=params)
    return response

def get_agent_details(token, agent_name):
    """
    Get detailed information about a specific Cortex agent
    
    Args:
        token: Bearer token for authentication
        agent_name: Name of the agent to describe
    """
    
    # API endpoint for describing a specific agent
    api_endpoint = f"https://eq06761.ap-southeast-2.snowflakecomputing.com/api/v2/databases/HOL2_DB/schemas/HOL2_SCHEMA/agents/{agent_name}"
    
    # Request headers
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    # Send the GET request
    response = requests.get(api_endpoint, headers=headers)
    return response

# Example usage
if __name__ == "__main__":
    token = os.getenv("SNOWFLAKE_TOKEN")
    
    # List all agents
    print("=== Listing all Cortex agents ===")
    response = list_cortex_agents(token=token)
    
    print(f"Status Code: {response.status_code}")
    print(f"Response Headers: {dict(response.headers)}")
    print(f"Response Text: {response.text}")
    
    # Try to parse JSON response if it exists
    if response.text.strip():
        try:
            print("JSON Response:")
            agents_data = response.json()
            print(json.dumps(agents_data, indent=2))
            
            # If we have agents, show some details
            if isinstance(agents_data, dict) and 'data' in agents_data:
                agents = agents_data['data']
                print(f"\nFound {len(agents)} agents:")
                for i, agent in enumerate(agents, 1):
                    print(f"{i}. Agent Name: {agent.get('name', 'N/A')}")
                    print(f"   Display Name: {agent.get('profile', {}).get('display_name', 'N/A')}")
                    print(f"   Comment: {agent.get('comment', 'N/A')}")
                    print()
        except json.JSONDecodeError:
            print("Response is not valid JSON")
    else:
        print("Response is empty")
    
    # Get details for a specific agent (if you want to test this)
    print("\n=== Getting details for a specific agent ===")
    specific_agent_response = get_agent_details(token=token, agent_name="custom_agent")
    
    print(f"Status Code: {specific_agent_response.status_code}")
    print(f"Response Text: {specific_agent_response.text}")
    
    if specific_agent_response.text.strip():
        try:
            print("Agent Details JSON:")
            print(json.dumps(specific_agent_response.json(), indent=2))
        except json.JSONDecodeError:
            print("Response is not valid JSON")

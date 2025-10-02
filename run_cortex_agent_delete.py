import requests
import json
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def delete_cortex_agent(token, agent_name):
    """
    Delete a Snowflake Cortex agent
    
    Args:
        token: Bearer token for authentication
        agent_name: Name of the agent to delete
    """
    
    # API endpoint - include agent name for deletion
    api_endpoint = f"https://eq06761.ap-southeast-2.snowflakecomputing.com/api/v2/databases/HOL2_DB/schemas/HOL2_SCHEMA/agents/{agent_name}"
    
    # Request headers
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    # Send the DELETE request
    response = requests.delete(api_endpoint, headers=headers)
    return response

# Example usage
if __name__ == "__main__":
    token = os.getenv("SNOWFLAKE_TOKEN")
    
    # Delete agent
    response = delete_cortex_agent(
        token=token,
        agent_name="custom_agent"
    )
    
    print(f"Status Code: {response.status_code}")
    print(f"Response Headers: {dict(response.headers)}")
    print(f"Response Text: {response.text}")
    
    # Try to parse JSON response if it exists
    if response.text.strip():
        try:
            print("JSON Response:")
            print(json.dumps(response.json(), indent=2))
        except json.JSONDecodeError:
            print("Response is not valid JSON")
    else:
        print("Response is empty")

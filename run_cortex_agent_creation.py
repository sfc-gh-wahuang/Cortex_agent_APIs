import requests
import json
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def create_cortex_agent(token, agent_name, semantic_view, 
                       search_service, 
                       warehouse):
    """
    Create a Snowflake Cortex agent
    
    Args:
        token: Bearer token for authentication
        agent_name: Name of the agent
        semantic_view: Path to the semantic view for the analyst tool
        search_service: Path to the search service
        warehouse: Warehouse name
    """
    
    # API endpoint
    api_endpoint = "https://eq06761.ap-southeast-2.snowflakecomputing.com/api/v2/databases/HOL2_DB/schemas/HOL2_SCHEMA/agents"
    
    # Request headers
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    # Request body
    payload = {
        "name": agent_name,
        "comment": "Sample agent",
        "profile": {
            "display_name": "My Data Agent",
            "avatar": "AiIcon",
            "color": "#00AEEF"
        },
        "models": {
            "orchestration": "CLAUDE-3-5-SONNET"
        },
        "orchestration": {
            "budget": {
                "seconds": 200,
                "tokens": 5000
            }
        },
        "instructions": {
            "response": "Always provide a concise response and maintain a friendly tone.",
            "orchestration": "",
            "system": "You are a helpful analyst.",
            "sample_questions": [
                {
                    "question": "Sample question",
                    "answer": "sample answer"
                }
            ]
        },
        "tools": [
            {
                "tool_spec": {
                    "description": "Analyst to analyze revenue",
                    "type": "cortex_analyst_text_to_sql",
                    "name": "Analyst1"
                }
            },
            {
                "tool_spec": {
                    "type": "cortex_search",
                    "name": "Search1"
                }
            }
        ],
        "tool_resources": {
            "Search1": {
                "search_service": search_service,
                "name": search_service,
                "max_results": 5
            },
            "Analyst1": {
                "semantic_view": semantic_view
            }
        },
        "warehouse": warehouse
    }
    
    # Send the request
    response = requests.post(api_endpoint, headers=headers, json=payload)
    return response

# Example usage
if __name__ == "__main__":
    token = os.getenv("SNOWFLAKE_TOKEN")
    
    # Create agent with custom settings
    response = create_cortex_agent(
        token=token,
        agent_name="custom_agent",
        semantic_view="HOL2_DB.HOL2_SCHEMA.REVENUE",
        search_service="HOL2_DB.HOL2_SCHEMA.PRODUCT_LINE_SEARCH_SERVICE",
        warehouse="HOL2_WH"
    )
    print(f"Status Code: {response.status_code}")
    print(json.dumps(response.json(), indent=2))
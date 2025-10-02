import requests
import json
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def parse_sse_events_readable(response):
    """
    Parse Server-Sent Events and display in a readable format
    """
    current_event = None
    response_text = ""
    thinking_text = ""
    
    print("Cortex Agent Response:")
    print("-" * 60)
    
    for line in response.iter_lines(decode_unicode=True):
        if line:
            if line.startswith('event:'):
                current_event = line[6:].strip()
                
                # Display status updates
                if current_event == 'response.status':
                    continue  # Will handle in data section
                elif current_event == 'response.text.delta':
                    continue  # Will accumulate text
                elif current_event == 'response.thinking.delta':
                    continue  # Will accumulate thinking
                elif current_event == 'done':
                    continue  # Will handle completion
                    
            elif line.startswith('data:'):
                data = line[5:].strip()
                
                # Check for stream completion
                if current_event == 'done' and data == '[DONE]':
                    print("\nResponse completed!")
                    break
                
                # Parse JSON data
                if data != '[DONE]':
                    try:
                        json_data = json.loads(data)
                        
                        # Handle different event types
                        if current_event == 'response.status':
                            status = json_data.get('status', '')
                            message = json_data.get('message', '')
                            print(f"Status: {message} ({status})")
                            
                        elif current_event == 'response.text.delta':
                            text_delta = json_data.get('text', '')
                            response_text += text_delta
                            print(text_delta, end='', flush=True)
                            
                        elif current_event == 'response.thinking.delta':
                            thinking_delta = json_data.get('text', '')
                            thinking_text += thinking_delta
                            
                        elif current_event == 'response.text':
                            # Final text content
                            final_text = json_data.get('text', '')
                            if final_text and not response_text:
                                print(f"\nResponse: {final_text}")
                                
                        elif current_event == 'response.thinking':
                            # Final thinking content
                            final_thinking = json_data.get('text', '')
                            if final_thinking:
                                print(f"\nAgent Thinking: {final_thinking[:200]}...")
                                
                        elif current_event == 'response.tool_use':
                            tool_name = json_data.get('name', 'Unknown')
                            print(f"\nUsing tool: {tool_name}")
                            
                        elif current_event == 'response.tool_result':
                            print(f"Tool completed")
                            
                        elif current_event == 'response.chart':
                            print(f"\nChart generated")
                            
                        elif current_event == 'response.table':
                            print(f"\nTable generated")
                            
                    except json.JSONDecodeError:
                        # Not JSON, might be plain text
                        if current_event == 'response.text.delta':
                            response_text += data
                            print(data, end='', flush=True)

def parse_sse_events_raw(response):
    """
    Parse Server-Sent Events from the streaming response (original version)
    """
    current_event = None
    
    for line in response.iter_lines(decode_unicode=True):
        if line:
            if line.startswith('event:'):
                current_event = line[6:].strip()
                print(f"Event: {current_event}")
                
            elif line.startswith('data:'):
                data = line[5:].strip()
                print(f"Data: {data}")
                
                # Check for stream completion
                if current_event == 'done' and data == '[DONE]':
                    print("Stream completed")
                    break
                    
                # Try to parse JSON data
                if data != '[DONE]':
                    try:
                        json_data = json.loads(data)
                        print(f"Parsed JSON: {json.dumps(json_data, indent=2)}")
                    except json.JSONDecodeError:
                        pass  # Not JSON, just print the raw data above

def run_cortex_agent(token, user_message, account_url,
                    semantic_view, 
                    # semantic_model_file,
                    search_service,
                    warehouse="HOL2_WH"):
    """
    Run a Cortex agent without creating an agent object
    
    Args:
        token: Bearer token for authentication
        user_message: The user's query/message to send to the agent
        account_url: Snowflake account URL
        semantic_view: Path to the semantic view for the analyst tool
        search_service: Path to the search service
        warehouse: Warehouse name
    """
    
    # API endpoint
    api_endpoint = f"{account_url}/api/v2/cortex/agent:run"
    
    # Request headers
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    # Request body
    payload = {
        "models": {
            "orchestration": "CLAUDE-3-5-SONNET"
        },
        "experimental": {
            "EnableRelatedQueries": True
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
            "system": "You are a helpful analyst."
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
                "search_service": f"{search_service}",
                "max_results": 5
            },
            "Analyst1": {
                "semantic_view": f"{semantic_view}",
                # "semantic_model_file": f"{semantic_model_file}",
                "execution_environment": {
                    "type": "warehouse",
                    "warehouse": "HOL2_WH"
                }
            }
        },
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": f"{user_message}"
                    }
                ]
            }
        ]
    }
    
    # Send the request
    response = requests.post(api_endpoint, headers=headers, json=payload, stream=True)
    return response

# Example usage
if __name__ == "__main__":
    token = os.getenv("SNOWFLAKE_TOKEN")
    
    # Run agent with custom settings and different query
    response = run_cortex_agent(
        token=token,
        user_message="How many users have used our products?",
        account_url="https://eq06761.ap-southeast-2.snowflakecomputing.com",
        semantic_view="HOL2_DB.HOL2_SCHEMA.REVENUE",
        # semantic_model_file="@HOL2_DB.HOL2_SCHEMA.SEMANTIC_MODEL/revenue_timeseries.yaml",
        search_service="HOL2_DB.HOL2_SCHEMA.PRODUCT_LINE_SEARCH_SERVICE",
        warehouse="HOL2_WH"
    )
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        # Use readable format by default, uncomment the line below for raw format
        parse_sse_events_readable(response)
        # parse_sse_events_raw(response)  # Uncomment for raw debug output
    else:
        print(f"Error: {response.status_code}")
        try:
            error_data = response.json()
            print(f"Error details: {json.dumps(error_data, indent=2)}")
        except:
            print(f"Error text: {response.text}")

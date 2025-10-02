# Snowflake Cortex Agent Run API - With Agent Object
# This script runs an existing Cortex agent object using the REST API

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
                    # Print any remaining accumulated text before completion
                    if response_text:
                        print(f"\n\nFinal Response: {response_text}")
                    print("\nResponse completed!")
                    break
                
                # Parse JSON data
                if data != '[DONE]':
                    try:
                        json_data = json.loads(data)
                        
                        # Handle different event types
                        if current_event == 'error':
                            error_code = json_data.get('code', 'Unknown')
                            error_message = json_data.get('message', 'Unknown error')
                            request_id = json_data.get('request_id', 'Unknown')
                            print(f"❌ ERROR: {error_message}")
                            print(f"   Error Code: {error_code}")
                            print(f"   Request ID: {request_id}")
                            
                        elif current_event == 'response.status':
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
                            if final_text:
                                print(f"\nResponse: {final_text}")
                                response_text = final_text  # Update accumulated text
                                
                        elif current_event == 'response.thinking':
                            # Final thinking content
                            final_thinking = json_data.get('text', '')
                            if final_thinking:
                                print(f"\nAgent Thinking: {final_thinking[:200]}...")
                                
                        elif current_event == 'response.tool_use':
                            tool_name = json_data.get('name', 'Unknown')
                            tool_type = json_data.get('type', 'Unknown')
                            print(f"\n🔧 Using tool: {tool_name} ({tool_type})")
                            
                        elif current_event == 'response.tool_result':
                            tool_name = json_data.get('name', 'Unknown')
                            status = json_data.get('status', 'Unknown')
                            print(f"✅ Tool {tool_name} completed with status: {status}")
                            
                        elif current_event == 'response.chart':
                            print(f"\n📊 Chart generated")
                            
                        elif current_event == 'response.table':
                            print(f"\n📋 Table generated")
                            
                        elif current_event == 'response.status':
                            status = json_data.get('status', '')
                            message = json_data.get('message', '')
                            print(f"📊 Status: {message} ({status})")
                            
                    except json.JSONDecodeError:
                        # Not JSON, might be plain text
                        if current_event == 'response.text.delta':
                            response_text += data
                            print(data, end='', flush=True)

def run_agent_object(token, agent_name, user_message, database, schema, 
                    account_url, thread_id=None, parent_message_id=None, tool_choice=None):
    """
    Run an existing Cortex agent object
    
    Args:
        token: Bearer token for authentication
        agent_name: Name of the existing agent to run
        user_message: The user's query/message to send to the agent
        database: Database name where the agent is stored
        schema: Schema name where the agent is stored
        account_url: Snowflake account URL
        thread_id: Optional thread ID for conversation continuity
        parent_message_id: Optional parent message ID (required if thread_id is provided)
    """
    
    # API endpoint for running an agent object
    api_endpoint = f"{account_url}/api/v2/databases/{database}/schemas/{schema}/agents/{agent_name}:run"
    
    # Request headers
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Build the request body according to documentation
    payload = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": user_message
                    }
                ]
            }
        ]
    }
    
    # Add thread information if provided
    if thread_id is not None:
        payload["thread_id"] = thread_id
        if parent_message_id is not None:
            payload["parent_message_id"] = parent_message_id
        else:
            # If thread_id is provided, parent_message_id is required
            payload["parent_message_id"] = 0  # Default to 0 for initial message
    
    # Add tool_choice if provided
    if tool_choice is not None:
        payload["tool_choice"] = tool_choice
    
    # Send the request
    response = requests.post(api_endpoint, headers=headers, json=payload, stream=True)
    return response

def run_agent_object_with_conversation_history(token, agent_name, conversation_history, 
                                              database, schema, account_url, tool_choice=None):
    """
    Run an existing Cortex agent object with full conversation history
    
    Args:
        token: Bearer token for authentication
        agent_name: Name of the existing agent to run
        conversation_history: List of messages in the conversation
        database: Database name where the agent is stored
        schema: Schema name where the agent is stored
        account_url: Snowflake account URL
    """
    
    # API endpoint for running an agent object
    api_endpoint = f"{account_url}/api/v2/databases/{database}/schemas/{schema}/agents/{agent_name}:run"
    
    # Request headers
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Request body with conversation history
    payload = {
        "messages": conversation_history
    }
    
    # Add tool_choice if provided
    if tool_choice is not None:
        payload["tool_choice"] = tool_choice
    
    # Send the request
    response = requests.post(api_endpoint, headers=headers, json=payload, stream=True)
    return response

# Example usage
if __name__ == "__main__":
    token = os.getenv("SNOWFLAKE_TOKEN")
    
    # Configuration - update these values for your environment
    agent_name = "custom_agent"  # Replace with your agent name
    database = "HOL2_DB"
    schema = "HOL2_SCHEMA"
    account_url = "https://eq06761.ap-southeast-2.snowflakecomputing.com"
    
    print("=== Running Agent Object ===")
    
    # Example 1: Simple single message with tool choice
    tool_choice = {
        "type": "auto",
        "name": ["Analyst1", "Search1"]  # Specify which tools to use
    }
    
    response = run_agent_object(
        token=token,
        agent_name=agent_name,
        user_message="What was the total revenue last quarter?",
        database=database,
        schema=schema,
        account_url=account_url,
        tool_choice=tool_choice
    )
    
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        # Use readable format by default
        parse_sse_events_readable(response)
    else:
        print(f"Error: {response.status_code}")
        try:
            error_data = response.json()
            print(f"Error details: {json.dumps(error_data, indent=2)}")
        except:
            print(f"Error text: {response.text}")
    
    # print("\n" + "="*60)
    # print("=== Running Agent with Conversation History ===")
    
    # # Example 2: Conversation with history
    # conversation_history = [
    #     {
    #         "role": "user",
    #         "content": [
    #             {
    #                 "type": "text",
    #                 "text": "What were our top 3 products by revenue last month?"
    #             }
    #         ]
    #     },
    #     {
    #         "role": "assistant",
    #         "content": [
    #             {
    #                 "type": "text",
    #                 "text": "Based on the data analysis, the top 3 products by revenue last month were: 1) Product A with $50K, 2) Product B with $45K, 3) Product C with $40K."
    #             }
    #         ]
    #     },
    #     {
    #         "role": "user",
    #         "content": [
    #             {
    #                 "type": "text",
    #                 "text": "Can you show me the trend for Product A over the last 6 months?"
    #             }
    #         ]
    #     }
    # ]
    
    # response2 = run_agent_object_with_conversation_history(
    #     token=token,
    #     agent_name=agent_name,
    #     conversation_history=conversation_history,
    #     database=database,
    #     schema=schema,
    #     account_url=account_url
    # )
    
    # print(f"Status Code: {response2.status_code}")
    
    # if response2.status_code == 200:
    #     parse_sse_events_readable(response2)
    # else:
    #     print(f"Error: {response2.status_code}")
    #     try:
    #         error_data = response2.json()
    #         print(f"Error details: {json.dumps(error_data, indent=2)}")
    #     except:
    #         print(f"Error text: {response2.text}")
    
    # print("\n" + "="*60)
    # print("=== Running Agent with Thread Continuity ===")
    
    # # Example 3: Using thread_id for conversation continuity
    # response3 = run_agent_object(
    #     token=token,
    #     agent_name=agent_name,
    #     user_message="Follow up on the previous analysis",
    #     database=database,
    #     schema=schema,
    #     account_url=account_url,
    #     thread_id=None,  # Use actual thread ID from previous conversation
    #     parent_message_id=None  # Use actual parent message ID
    # )
    
    # print(f"Status Code: {response3.status_code}")
    
    # if response3.status_code == 200:
    #     parse_sse_events_readable(response3)
    # else:
    #     print(f"Error: {response3.status_code}")
    #     try:
    #         error_data = response3.json()
    #         print(f"Error details: {json.dumps(error_data, indent=2)}")
    #     except:
    #         print(f"Error text: {response3.text}")

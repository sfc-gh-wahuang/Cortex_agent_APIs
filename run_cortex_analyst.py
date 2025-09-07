import requests
import json
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def parse_analyst_sse_events(response):
    """
    Parse Server-Sent Events from Cortex Analyst streaming response
    """
    current_event = None
    content_blocks = {}  # Track content by index
    
    print("Cortex Analyst Response:")
    print("-" * 60)
    
    for line in response.iter_lines(decode_unicode=True):
        if line:
            if line.startswith('event:'):
                current_event = line[6:].strip()
                
            elif line.startswith('data:'):
                data = line[5:].strip()
                
                # Check for stream completion
                if current_event == 'done':
                    print("\nAnalyst response completed!")
                    break
                
                # Parse JSON data
                try:
                    json_data = json.loads(data)
                    
                    # Handle different event types
                    if current_event == 'status':
                        status = json_data.get('status', '')
                        print(f"Status: {status}")
                        
                    elif current_event == 'message.content.delta':
                        index = json_data.get('index', 0)
                        content_type = json_data.get('type', '')
                        
                        # Initialize content block if not exists
                        if index not in content_blocks:
                            content_blocks[index] = {'type': content_type, 'content': ''}
                        
                        if content_type == 'text':
                            text_delta = json_data.get('text_delta', '')
                            content_blocks[index]['content'] += text_delta
                            print(text_delta, end='', flush=True)
                            
                        elif content_type == 'sql':
                            statement_delta = json_data.get('statement_delta', '')
                            content_blocks[index]['content'] += statement_delta
                            if statement_delta:
                                print(f"\nSQL: {statement_delta}")
                                
                        elif content_type == 'suggestions':
                            suggestions_delta = json_data.get('suggestions_delta', {})
                            suggestion_index = suggestions_delta.get('index', 0)
                            suggestion_delta = suggestions_delta.get('suggestion_delta', '')
                            
                            if 'suggestions' not in content_blocks[index]:
                                content_blocks[index]['suggestions'] = {}
                            if suggestion_index not in content_blocks[index]['suggestions']:
                                content_blocks[index]['suggestions'][suggestion_index] = ''
                                
                            content_blocks[index]['suggestions'][suggestion_index] += suggestion_delta
                            print(f"\nSuggestion {suggestion_index + 1}: {suggestion_delta}", end='', flush=True)
                    
                    elif current_event == 'warnings':
                        warnings = json_data.get('warnings', [])
                        for warning in warnings:
                            print(f"\nWarning: {warning.get('message', '')}")
                    
                    elif current_event == 'response_metadata':
                        model_names = json_data.get('model_names', [])
                        question_category = json_data.get('question_category', '')
                        print(f"\nMetadata - Models: {model_names}, Category: {question_category}")
                    
                    elif current_event == 'error':
                        error_message = json_data.get('message', '')
                        error_code = json_data.get('code', '')
                        print(f"\nError: {error_message} (Code: {error_code})")
                        break
                        
                except json.JSONDecodeError:
                    print(f"Raw data: {data}")

def send_analyst_message(token, question, account_url, semantic_model_file=None, 
                        semantic_view=None, semantic_model_spec=None, stream=True,
                        conversation_history=None):
    """
    Send a message to Cortex Analyst
    
    Args:
        token: Bearer token for authentication
        question: The user's natural language question
        account_url: Snowflake account URL
        semantic_model_file: Path to YAML file on stage (e.g., "@stage/model.yaml")
        semantic_view: Name of semantic view (e.g., "db.schema.view")
        semantic_model_spec: Direct YAML specification as string
        stream: Whether to use streaming response
        conversation_history: List of previous messages for multi-turn conversation
    
    Returns:
        requests.Response: The response object
    """
    
    # API endpoint
    api_endpoint = f"{account_url}/api/v2/cortex/analyst/message"
    
    # Request headers
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Build messages array
    messages = []
    
    # Add conversation history if provided
    if conversation_history:
        messages.extend(conversation_history)
    
    # Add current user question
    user_message = {
        "role": "user",
        "content": [
            {
                "type": "text",
                "text": question
            }
        ]
    }
    messages.append(user_message)
    
    # Build request payload
    payload = {
        "messages": messages,
        "stream": stream
    }
    
    # Add semantic model configuration (only one should be provided)
    if semantic_model_file:
        payload["semantic_model_file"] = semantic_model_file
    elif semantic_view:
        payload["semantic_view"] = semantic_view
    elif semantic_model_spec:
        payload["semantic_model_spec"] = semantic_model_spec
    else:
        raise ValueError("Must provide one of: semantic_model_file, semantic_view, or semantic_model_spec")
    
    # Send the request
    response = requests.post(api_endpoint, headers=headers, json=payload, stream=stream)
    return response

def send_analyst_feedback(token, request_id, positive, feedback_message, account_url):
    """
    Send feedback for a Cortex Analyst response
    
    Args:
        token: Bearer token for authentication
        request_id: The request ID from the analyst response
        positive: True for positive feedback, False for negative
        feedback_message: Optional feedback message
        account_url: Snowflake account URL
    
    Returns:
        requests.Response: The response object
    """
    
    # API endpoint
    api_endpoint = f"{account_url}/api/v2/cortex/analyst/feedback"
    
    # Request headers
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Request payload
    payload = {
        "request_id": request_id,
        "positive": positive
    }
    
    if feedback_message:
        payload["feedback_message"] = feedback_message
    
    # Send the request
    response = requests.post(api_endpoint, headers=headers, json=payload)
    return response

def analyst_non_streaming_example(token, question, account_url, semantic_model_file):
    """
    Example of non-streaming Cortex Analyst request
    """
    response = send_analyst_message(
        token=token,
        question=question,
        account_url=account_url,
        semantic_model_file=semantic_model_file,
        stream=False
    )
    
    if response.status_code == 200:
        result = response.json()
        print("Non-streaming Response:")
        print("-" * 40)
        print(f"Request ID: {result.get('request_id', 'N/A')}")
        
        message = result.get('message', {})
        content = message.get('content', [])
        
        for i, block in enumerate(content):
            block_type = block.get('type', '')
            print(f"\nContent Block {i + 1} ({block_type}):")
            
            if block_type == 'text':
                print(f"Text: {block.get('text', '')}")
            elif block_type == 'sql':
                print(f"SQL: {block.get('statement', '')}")
                confidence = block.get('confidence', {})
                if confidence:
                    print(f"Confidence info: {confidence}")
            elif block_type == 'suggestions':
                suggestions = block.get('suggestions', [])
                print("Suggestions:")
                for j, suggestion in enumerate(suggestions, 1):
                    print(f"  {j}. {suggestion}")
        
        # Display warnings if any
        warnings = result.get('warnings', [])
        if warnings:
            print("\nWarnings:")
            for warning in warnings:
                print(f"  - {warning.get('message', '')}")
        
        # Display metadata
        metadata = result.get('response_metadata', {})
        if metadata:
            print(f"\nMetadata:")
            print(f"  Models: {metadata.get('model_names', [])}")
            print(f"  Question Category: {metadata.get('question_category', 'N/A')}")
        
        return result.get('request_id')
    else:
        print(f"Error: {response.status_code}")
        print(f"Response: {response.text}")
        return None

# Example usage
if __name__ == "__main__":
    token = os.getenv("SNOWFLAKE_TOKEN")
    
    # Configuration
    account_url = "https://eq06761.ap-southeast-2.snowflakecomputing.com"
    semantic_model_file = "@HOL2_DB.HOL2_SCHEMA.SEMANTIC_MODEL/revenue_timeseries.yaml"
    # semantic_view = "HOL2_DB.HOL2_SCHEMA.REVENUE_VIEW"  # Alternative to semantic_model_file
    
    print("=== Cortex Analyst Examples ===")
    
    # Example 1: Streaming request
    print("\n1. Streaming Request:")
    question1 = "What was the total revenue last quarter?"
    
    response = send_analyst_message(
        token=token,
        question=question1,
        account_url=account_url,
        semantic_model_file=semantic_model_file,
        stream=True
    )
    
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        parse_analyst_sse_events(response)
    else:
        print(f"Error: {response.status_code}")
        print(f"Response: {response.text}")
    
    print("\n" + "="*60)
    
    # Example 2: Non-streaming request
    print("\n2. Non-streaming Request:")
    question2 = "Show me the top 5 products by revenue"
    
    request_id = analyst_non_streaming_example(
        token=token,
        question=question2,
        account_url=account_url,
        semantic_model_file=semantic_model_file
    )
    
    print("\n" + "="*60)
    
    # Example 3: Multi-turn conversation
    print("\n3. Multi-turn Conversation:")
    
    # First question
    conversation_history = []
    
    response1 = send_analyst_message(
        token=token,
        question="What were our sales last month?",
        account_url=account_url,
        semantic_model_file=semantic_model_file,
        stream=False,
        conversation_history=conversation_history
    )
    
    if response1.status_code == 200:
        result1 = response1.json()
        print("First question response received")
        
        # Add to conversation history
        conversation_history.append({
            "role": "user",
            "content": [{"type": "text", "text": "What were our sales last month?"}]
        })
        conversation_history.append(result1.get('message', {}))
        
        # Follow-up question
        response2 = send_analyst_message(
            token=token,
            question="How does that compare to the previous month?",
            account_url=account_url,
            semantic_model_file=semantic_model_file,
            stream=True,
            conversation_history=conversation_history
        )
        
        if response2.status_code == 200:
            print("\nFollow-up question (streaming):")
            parse_analyst_sse_events(response2)
    
    print("\n" + "="*60)
    
    # Example 4: Send feedback (commented out - uncomment to use)
    # if request_id:
    #     print("\n4. Sending Feedback:")
    #     feedback_response = send_analyst_feedback(
    #         token=token,
    #         request_id=request_id,
    #         positive=True,
    #         feedback_message="Great analysis! Very helpful.",
    #         account_url=account_url
    #     )
    #     
    #     if feedback_response.status_code == 200:
    #         print("Feedback sent successfully!")
    #     else:
    #         print(f"Failed to send feedback: {feedback_response.status_code}")
    #         print(f"Response: {feedback_response.text}")
    
    print("\nCortex Analyst examples completed.")

import asyncio
import json
import time
from config import settings
from .client_manager import HTTPClientManager
from .json_utils import extract_json

async def call_ollama(
    client: HTTPClientManager,
    prompt: str,
    model: str,
    max_retries: int
) -> str:
    """
    Sends prompt to Ollama API with retry logic.
    Returns a JSON-stringified dict of results.
    """
    start_time = time.time()
    print("\n🤖 Starting Ollama API Call")
    print("------------------------")
    print(f"Model: {model}")
    print(f"Max retries: {max_retries}")
    print(f"Prompt length: {len(prompt)} characters")
    print(f"API URL: {settings.OLLAMA_API_URL}")
    
    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a deterministic medical research screening assistant. "
                    "Respond with ONLY valid JSON in the exact format requested, nothing else."
                )
            },
            {"role": "user", "content": prompt}
        ],
        "stream": False,
        "options": {
            "temperature": 0.1,
            "num_predict": 4000,
            "num_ctx": 2048,
            "num_thread": 4,
            "stop": ["}"] # Stop after closing brace to prevent extra content
        }
    }

    print("\n📤 Request Details:")
    print(f"Payload size: {len(json.dumps(payload))} bytes")
    print(f"System prompt: {payload['messages'][0]['content']}")
    print(f"User prompt (truncated): {prompt[:200]}...")

    last_exc: Exception | None = None

    for attempt in range(max_retries + 1):
        try:
            print(f"\n🔄 Attempt {attempt + 1}/{max_retries + 1}")
            print(f"Time elapsed: {time.time() - start_time:.2f}s")
            
            # Send request with increased timeout
            response = await asyncio.wait_for(
                client.post(
                    f"{settings.OLLAMA_API_URL}/api/chat",
                    json=payload,
                    headers={"Content-Type": "application/json"}
                ),
                timeout=600.0  # 10 minutes timeout
            )
            
            print(f"\n📥 Response received ({time.time() - start_time:.2f}s)")
            print(f"Status code: {response.status_code}")
            print(f"Response headers: {dict(response.headers)}")
            
            if response.status_code == 404:
                raise Exception(f"Ollama API endpoint not found")

            if response.status_code != 200:
                print(f"Response text: {response.text[:1000]}")
                raise Exception(f"HTTP {response.status_code}: {response.text[:500]}")
            
            try:
                data = response.json()
                print("\n✅ Parsed JSON response successfully")
                print(f"Response structure: {json.dumps(data, indent=2)[:500]}...")
            except Exception as json_exc:
                print(f"\n❌ JSON parsing failed: {str(json_exc)}")
                print(f"Raw response (truncated): {response.text[:1000]}")
                raise ValueError(f"Invalid JSON response: {str(json_exc)}")
            
            if not isinstance(data, dict) or 'message' not in data:
                print(f"Unexpected response format: {json.dumps(data, indent=2)[:500]}")
                raise ValueError(f"Invalid response format - missing 'message' field")

            content = data['message'].get('content', '')
            print(f"\n📝 LLM Response Content:")
            print("---------------------")
            print(content[:2000])
            print("..." if len(content) > 2000 else "")
            
            if not content:
                raise ValueError("Empty content in response")

            # Extract and validate JSON
            clean = extract_json(content)
            if not clean:
                print("\n❌ Failed to extract valid JSON from response")
                print("Raw content:")
                print(content)
                raise ValueError("Could not extract valid JSON from response")
            
            print("\n🔍 Extracted Results:")
            print("-----------------")
            print(json.dumps(clean, indent=2)[:2000])
            print("..." if len(json.dumps(clean)) > 2000 else "")
            
            # Validate result structure
            for article_id, result in clean.items():
                if not isinstance(result, dict):
                    raise ValueError(f"Invalid result format for article {article_id}")
                if "included" not in result or "reason" not in result or "relevanceScore" not in result:
                    raise ValueError(f"Missing required fields in result for article {article_id}")
            
            print(f"\n✨ Request completed successfully in {time.time() - start_time:.2f}s")
            return json.dumps(clean)

        except asyncio.TimeoutError:
            print(f"\n⏱️ Request timed out after 600s (attempt {attempt + 1})")
            if attempt == max_retries:
                print("✅ Request accepted, continuing in background")
                return json.dumps({"status": "processing"})
            last_exc = asyncio.TimeoutError("Request timed out")
            
        except (asyncio.CancelledError, KeyboardInterrupt) as e:
            print(f"\n🛑 Operation cancelled: {type(e).__name__}")
            raise
            
        except Exception as e:
            last_exc = e
            print(f"\n❌ Error on attempt {attempt + 1}: {str(e)}")
            print(f"Exception type: {type(e).__name__}")
            print(f"Exception details: {str(e)}")
            
        if attempt < max_retries:
            retry_delay = 10 * (attempt + 1)
            print(f"\n⏳ Retrying in {retry_delay} seconds...")
            await asyncio.sleep(retry_delay)
            continue
            
        print(f"\n🚫 All {max_retries + 1} attempts failed")
        print(f"Final error: {str(last_exc)}")
        break

    print("\n⚠️ Continuing processing in background")
    return json.dumps({"status": "processing"})
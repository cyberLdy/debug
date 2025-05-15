
import httpx
import json
import asyncio
from typing import Dict, Any
from config import settings

class LLMService:
    def __init__(self):
        self._cancelled = False
        self._current_task: asyncio.Task | None = None
        self._timeout = 30.0  # 30 seconds timeout
        self._max_retries = 2
        self._client: httpx.AsyncClient | None = None
        self._request_lock = asyncio.Lock()

    async def initialize(self):
        """Initialize HTTP client"""
        if not self._client:
            self._client = httpx.AsyncClient(
                timeout=self._timeout,
                limits=httpx.Limits(max_keepalive_connections=5, max_connections=10)
            )

    async def cleanup(self):
        """Cleanup resources"""
        if self._client:
            await self._client.aclose()
            self._client = None

    def cancel(self):
        """Cancel any ongoing operations"""
        print("🛑 LLM Service: Cancelling operations")
        self._cancelled = True
        if self._current_task and not self._current_task.done():
            print("🛑 LLM Service: Cancelling current API request")
            self._current_task.cancel()

    async def generate_response(self, prompt: str, model: str) -> str:
        """Generate response from LLM model with cancellation support"""
        print(f"\n🤖 Calling LLM API with model: {model}")
        print(f"📝 Prompt length: {len(prompt)} characters")
        
        self._cancelled = False
        
        try:
            await self.initialize()
            print("🔄 Using Ollama API")

            async with self._request_lock:  # Prevent concurrent requests
                self._current_task = asyncio.create_task(
                    self._call_ollama(prompt, model),
                    name="ollama_api_call"
                )

                try:
                    return await self._current_task
                except asyncio.CancelledError:
                    print("🛑 API request cancelled")
                    raise
                finally:
                    self._current_task = None

        except asyncio.CancelledError:
            print("🛑 Operation cancelled during API call")
            raise
        except Exception as e:
            print(f"❌ API call error: {str(e)}")
            raise
        finally:
            await self.cleanup()

    async def _call_ollama(self, prompt: str, model: str) -> str:
        """Call Ollama API with optimized settings"""
        if not self._client:
            raise RuntimeError("HTTP client not initialized")

        try:
            print(f"\n🤖 Calling Ollama API with model: {model}")
            print(f"📝 Prompt length: {len(prompt)} characters")
            
            if self._cancelled:
                print("🛑 Request cancelled before sending")
                raise asyncio.CancelledError("Operation cancelled")

            payload = {
                "model": model,
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a deterministic medical research screening assistant. You must respond with ONLY valid JSON in the exact format requested, nothing else."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "stream": False,
                "options": {
                    "temperature": 0.1,
                    "num_predict": 4000,
                    "num_ctx": 2048,
                    "num_thread": 4
                }
            }

            print(f"🌐 Sending request to {settings.OLLAMA_API_URL}")
            
            for attempt in range(self._max_retries + 1):
                try:
                    print(f"\n🔄 Attempt {attempt + 1}/{self._max_retries + 1}")
                    response = await self._client.post(
                        f"{settings.OLLAMA_API_URL}/api/chat",
                        json=payload,
                        headers={'Content-Type': 'application/json'}
                    )
                    
                    if self._cancelled:
                        print("🛑 Request cancelled after response")
                        raise asyncio.CancelledError("Operation cancelled")
                    
                    if response.status_code == 404:
                        print(f"❌ Ollama API HTTP error: {response.status_code}")
                        print(f"Response text: {response.text}")
                        if attempt < self._max_retries:
                            print(f"⏳ Retrying in {10 * (attempt + 1)} seconds...")
                            await asyncio.sleep(10 * (attempt + 1))
                            continue
                        raise Exception(f"Ollama API error: {response.status_code} {response.text}")

                    # Parse response
                    data = response.json()

                    if not isinstance(data, dict) or 'message' not in data:
                        print(f"❌ Invalid Ollama response format: {data}")
                        raise ValueError("Invalid response format from Ollama")
                    
                    content = data['message'].get('content', '')
                    if not content:
                        print("❌ Empty content in Ollama response")
                        raise ValueError("Empty response from Ollama")

                    # Extract and validate JSON
                    json_data = self._extract_json(content)
                    if not json_data:
                        raise ValueError("Could not extract valid JSON from response")

                    # Validate the extracted JSON is a dictionary
                    if not isinstance(json_data, dict):
                        print(f"❌ Invalid JSON structure: {json_data}")
                        raise ValueError("Response must be a dictionary")

                    # Validate required fields for each article
                    for article_id, result in json_data.items():
                        if not isinstance(result, dict):
                            print(f"❌ Invalid result format for article {article_id}: {result}")
                            raise ValueError(f"Result for article {article_id} must be a dictionary")

                        # Ensure required fields are present
                        required_fields = ['included', 'reason', 'relevanceScore']
                        missing_fields = [field for field in required_fields if field not in result]
                        if missing_fields:
                            print(f"❌ Missing required fields for article {article_id}: {missing_fields}")
                            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")

                        # Validate field types
                        if not isinstance(result['included'], bool):
                            result['included'] = bool(result['included'])
                        if not isinstance(result['reason'], str):
                            result['reason'] = str(result['reason'])
                        if not isinstance(result['relevanceScore'], (int, float)):
                            try:
                                result['relevanceScore'] = float(result['relevanceScore'])
                            except (ValueError, TypeError):
                                result['relevanceScore'] = 0.0

                        # Ensure score is within bounds
                        result['relevanceScore'] = max(0, min(100, float(result['relevanceScore'])))

                    print("\n✅ Final validated JSON:")
                    print(json.dumps(json_data, indent=2))
                    return json.dumps(json_data)

                except httpx.TimeoutException:
                    print(f"⚠️ Request timeout on attempt {attempt + 1}")
                    if attempt < self._max_retries:
                        print(f"⏳ Retrying in {1 * (attempt + 1)} seconds...")
                        await asyncio.sleep(1 * (attempt + 1))
                        continue
                    raise

            print("❌ All retry attempts failed")
            raise Exception("All retry attempts failed")

        except asyncio.CancelledError:
            print("🛑 Operation cancelled")
            raise
        except httpx.TimeoutException:
            print("❌ Ollama API timeout")
            raise Exception("Ollama API request timed out")
        except Exception as e:
            print(f"❌ Ollama API error: {str(e)}")
            raise

    def _extract_json(self, content: str) -> Dict[str, Any]:

        
        content = content.strip()
        
        # Try parsing as pure JSON first
        try:
            json_data = json.loads(content)
            print("✅ Successfully parsed as pure JSON")
            return json_data
        except json.JSONDecodeError as e:
            print(f"⚠️ Pure JSON parse failed: {e}")
            pass

        # Remove markdown code block markers if present
        if content.startswith("```json"):
            print("📝 Removing ```json prefix")
            content = content[7:]
        elif content.startswith("```"):
            print("📝 Removing ``` prefix")
            content = content[3:]
        if content.endswith("```"):
            print("📝 Removing ``` suffix")
            content = content[:-3]
        
        content = content.strip()
        print("\n📝 Cleaned content:")
        print(content)
        
        # Find JSON object boundaries
        start = content.find('{')
        end = content.rfind('}')
        
        if start >= 0 and end > start:
            try:
                json_str = content[start:end + 1]
                print("\n📝 Extracted JSON string:")
                print(json_str)
                
                json_data = json.loads(json_str)
                print("✅ Successfully parsed extracted JSON")
                return json_data
            except json.JSONDecodeError as e:
                print(f"❌ Failed to parse extracted JSON: {e}")
                print(f"Extracted content: {content[start:end + 1]}")
                return {}
        
        print(f"❌ No valid JSON found in response: {content}")
        return {}

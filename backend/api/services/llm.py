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
        self._score_threshold = 60  # New threshold for decision validation

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

                    # Validate and correct inconsistencies between score and decision
                    json_data = self._validate_decisions(json_data)

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

                        # Validate field types with more explicit type handling
                        # Handle 'included' field - convert strings "true"/"false" properly
                        if isinstance(result['included'], str):
                            result['included'] = result['included'].lower() == "true"
                        elif not isinstance(result['included'], bool):
                            result['included'] = bool(result['included'])
                            
                        # Handle reason field
                        if not isinstance(result['reason'], str):
                            result['reason'] = str(result['reason'])
                            
                        # Handle score - convert percentages, handle strings, etc.
                        if isinstance(result['relevanceScore'], str):
                            # Remove any % sign if present
                            score_str = result['relevanceScore'].replace('%', '').strip()
                            try:
                                result['relevanceScore'] = float(score_str)
                            except (ValueError, TypeError):
                                print(f"⚠️ Could not parse score: {result['relevanceScore']}")
                                result['relevanceScore'] = 0.0
                        elif not isinstance(result['relevanceScore'], (int, float)):
                            try:
                                result['relevanceScore'] = float(result['relevanceScore'])
                            except (ValueError, TypeError):
                                print(f"⚠️ Could not convert score to float: {result['relevanceScore']}")
                                result['relevanceScore'] = 0.0

                        # Ensure score is within bounds (0-100)
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

    def _validate_decisions(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and correct inconsistencies between score and inclusion decision"""
        print("\n🔍 Validating decision logic based on scores...")
        corrections_made = False
        
        for article_id, result in json_data.items():
            if not isinstance(result, dict):
                continue
                
            if 'relevanceScore' not in result or 'included' not in result:
                continue
                
            # Convert score to float and round to handle potential floating point issues
            try:
                score = float(result['relevanceScore'])
                score = round(score, 1)  # Round to 1 decimal place for stable comparison
            except (ValueError, TypeError):
                print(f"⚠️ Invalid score format for article {article_id}: {result['relevanceScore']}")
                continue
                
            # Convert current_decision to boolean, handling string values like "true"/"false"
            if isinstance(result['included'], str):
                current_decision = result['included'].lower() == "true"
            else:
                current_decision = bool(result['included'])
            
            # Force exact comparison - article MUST be included if score >= threshold
            # Print detailed debug info for every article regardless of correction
            print(f"📊 Article {article_id}:")
            print(f"   - Score: {score}")
            print(f"   - Threshold: {self._score_threshold}")
            print(f"   - Current decision: {'included' if current_decision else 'excluded'}")
            
            # Determine correct decision based on threshold
            correct_decision = score >= self._score_threshold
            
            print(f"   - Correct decision should be: {'included' if correct_decision else 'excluded'}")
            print(f"   - Need correction: {current_decision != correct_decision}")
            
            if current_decision != correct_decision:
                old_decision = "included" if current_decision else "excluded"
                new_decision = "included" if correct_decision else "excluded"
                print(f"⚠️ CORRECTING decision-score mismatch for article {article_id}:")
                print(f"   - Score: {score}")
                print(f"   - Current decision: {old_decision}")
                print(f"   - Corrected to: {new_decision}")
                
                # Update the decision - make sure it's a proper boolean
                result['included'] = correct_decision
                
                # Update the reason prefix if needed
                if 'reason' in result and isinstance(result['reason'], str):
                    reason = result['reason']
                    if reason.startswith("Included:") and not correct_decision:
                        reason = "Excluded:" + reason[9:]
                    elif reason.startswith("Excluded:") and correct_decision:
                        reason = "Included:" + reason[9:]
                    result['reason'] = reason
                
                corrections_made = True
            
        if corrections_made:
            print("✅ Decision corrections applied based on score threshold")
        else:
            print("✓ No decision corrections needed, all decisions match scores")
            
        return json_data

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
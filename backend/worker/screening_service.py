import json
from typing import List, Dict, Any
from api.services.llm import LLMService
from api.services.prompts import build_screening_prompt

class ScreeningService:
    def __init__(self):
        self.llm_service = LLMService()

    async def screen_batch(
        self,
        articles: List[Dict],
        criteria: str,
        model: str
    ) -> Dict[str, Any]:
        """Screen a batch of articles using LLM"""
        print(f"🤖 Screening batch of {len(articles)} articles with model: {model}")
        
        try:
            # Build prompt and get LLM response
            prompt = build_screening_prompt(articles, criteria)
            print(f"📝 Prompt length: {len(prompt)} characters")
            print(f"🔄 Using Ollama API")
            
            response = await self.llm_service.generate_response(prompt, model)

            # Parse & validate JSON
            data = json.loads(response)
            if not isinstance(data, dict):
                raise ValueError("Results must be a dictionary")

            # Validate each result
            validated: Dict[str, Any] = {}
            for aid, res in data.items():
                if not isinstance(res, dict):
                    continue

                # Ensure required fields with correct types
                included = bool(res.get("included", False))
                reason = str(res.get("reason", ""))
                score = float(res.get("relevanceScore", 0))
                score = max(0, min(100, score))

                validated[aid] = {
                    "included": included,
                    "reason": reason,
                    "relevanceScore": score
                }

            print(f"✅ LLM response: Successfully screened {len(validated)} articles")
            print("📊 Results summary:")
            included_count = sum(1 for r in validated.values() if r['included'])
            excluded_count = len(validated) - included_count
            print(f"  • Included: {included_count}")
            print(f"  • Excluded: {excluded_count}")
            
            # Print details for each article
            for aid, res in validated.items():
                print(f"  • Article {aid}: {'✅ Included' if res['included'] else '❌ Excluded'} (score: {res['relevanceScore']})")

            return validated

        except json.JSONDecodeError as je:
            print(f"Error parsing screening response: {je}")
            print(f"Raw response: {response}")
            raise ValueError(f"Failed to parse screening response: {je}")
        except Exception as e:
            print(f"LLM error: {e}")
            raise
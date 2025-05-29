import json
from typing import Dict, Any

def extract_json(content: str) -> Dict[str, Any]:
    """
    Extracts a JSON object from raw string, stripping markdown fences if needed.
    """
    text = content.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # strip code fences
        if text.startswith("```json"):
            text = text[7:]
        elif text.startswith("```"):
            text = text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

        start = text.find('{')
        end = text.rfind('}')
        if 0 <= start < end:
            try:
                return json.loads(text[start:end+1])
            except json.JSONDecodeError:
                pass
        return {}
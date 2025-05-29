def build_screening_prompt(articles: list, criteria: str) -> str:
    """Build prompt for LLM screening"""
    return f"""You are a precise and deterministic medical research screening assistant. Analyze these articles based on the given criteria and provide clear results.

SCREENING CRITERIA:
{criteria}

STRICT SCORING RULES:
1. Relevance Score (0-100):
   - 90-100: Perfect match with all criteria
   - 70-89: Strong match with most criteria
   - 50-69: Moderate match with some criteria
   - 30-49: Weak match with few criteria
   - 0-29: Very poor match or irrelevant

2. Reason Format:
   - Start with "Included:" or "Excluded:"
   - List specific matching/missing criteria
   - Be concise but specific

ARTICLES TO ANALYZE:
{format_articles(articles)}

REQUIRED OUTPUT FORMAT:
{{
  "article_id": {{
    "included": boolean,
    "reason": "string explaining decision",
    "relevanceScore": number (0-100)
  }}
}}

CRITICAL REQUIREMENTS:
1. Response MUST be a JSON object (dictionary), NOT an array
2. Each article must be a key-value pair in the root object
3. Use the article ID as the key for each result
4. Each result must have exactly these fields:
   - included: true/false
   - reason: string
   - relevanceScore: number 0-100
5. Example format:
   {{
     "12345": {{
       "included": true,
       "reason": "Included: Matches all criteria...",
       "relevanceScore": 85
     }},
     "67890": {{
       "included": false,
       "reason": "Excluded: Does not match...",
       "relevanceScore": 30
     }}
   }}
  
IMPORTANT DECISION LOGIC:
- If relevanceScore >= 70 → included = true
- If relevanceScore < 70 → included = false
- This rule is NON-NEGOTIABLE and MUST be followed
- Do NOT override this rule based on other reasoning


IMPORTANT: NEVER return a list/array! Always return a dictionary/object with article IDs as keys."""

def format_articles(articles: list) -> str:
    """Format articles for prompt"""
    return "\n\n".join([
        f"Article ID: {article['id']}\n"
        f"Title: {article['title']}\n"
        f"Abstract: {article['abstract']}"
        for article in articles
    ])
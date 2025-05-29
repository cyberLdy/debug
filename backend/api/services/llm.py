import asyncio
from api.services.llm_part.client_manager import HTTPClientManager
from api.services.llm_part.ollama_call import call_ollama



class LLMService:
    """
    High-level interface for generating JSON-based screening results from an LLM.
    """
    def __init__(self, timeout: float = 30.0, retries: int = 2):
        self._cancelled = False
        self._current_task: asyncio.Task | None = None
        self._client_manager = HTTPClientManager(timeout)
        self._max_retries = retries

    async def initialize(self):
        await self._client_manager.initialize()

    async def cleanup(self):
        await self._client_manager.cleanup()

    def cancel(self):
        self._cancelled = True
        if self._current_task and not self._current_task.done():
            self._current_task.cancel()

    async def generate_response(self, prompt: str, model: str) -> str:
        """
        Calls the Ollama API via `call_ollama`, ensuring init/cleanup.
        Returns JSON-stringified screening results.
        """
        self._cancelled = False
        try:
            await self.initialize()
            self._current_task = asyncio.create_task(
                call_ollama(self._client_manager, prompt, model, self._max_retries)
            )
            return await self._current_task
        finally:
            await self.cleanup()
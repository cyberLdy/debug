from pydantic_settings import BaseSettings
import os
from pathlib import Path
from dotenv import load_dotenv
from urllib.parse import urlparse

env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

class Settings(BaseSettings):
    # MongoDB settings - these should never be modified by env reload
    MONGODB_URI: str = os.getenv('MONGODB_URI', 'mongodb://devchat_user:future_456@10.14.16.60:5200/dongyu?authSource=admin')
    MONGODB_DB: str = urlparse(MONGODB_URI).path.lstrip('/').split('?')[0]
    
    # LLM API settings - these can be reloaded
    OLLAMA_API_URL: str = os.getenv('NEXT_PUBLIC_OLLAMA_API_URL', '')
    OLLAMA_MODEL: str = os.getenv('NEXT_PUBLIC_OLLAMA_MODEL', '')

    # Screening settings
    ARTICLE_LIMIT: int = 10
    
    # Service settings
    BATCH_SIZE: int = 2
    MAX_RETRIES: int = 2
    RETRY_DELAY: int = 2
    REQUEST_TIMEOUT: int = 120

    # Track last env file modification time
    _env_mtime: float = 0

    def reload_if_changed(self) -> bool:
        """Reload environment variables if .env.local has changed"""
        try:
            if not env_path.exists():
                return False

            current_mtime = env_path.stat().st_mtime
            if current_mtime > self._env_mtime:
                print(f"\n🔄 Detected changes in {env_path}, reloading configuration...")
                
                # Load new environment variables
                load_dotenv(env_path, override=True)
                
                # Update settings
                self.OLLAMA_API_URL = os.getenv('NEXT_PUBLIC_OLLAMA_API_URL', self.OLLAMA_API_URL)
                self.OLLAMA_MODEL = os.getenv('NEXT_PUBLIC_OLLAMA_MODEL', self.OLLAMA_MODEL)
                self.ARTICLE_LIMIT = int(os.getenv('ARTICLE_LIMIT', str(self.ARTICLE_LIMIT)))
                
                # Update modification time
                self._env_mtime = current_mtime
                
                print("✅ Configuration reloaded successfully")
                print("📝 Current settings:")
                print(f"  - Ollama URL: {self.OLLAMA_API_URL}")
                print(f"  - Ollama Model: {self.OLLAMA_MODEL}")
                print(f"  - Article Limit: {self.ARTICLE_LIMIT}")
                return True
                
            return False
            
        except Exception as e:
            print(f"❌ Error reloading configuration: {e}")
            return False

    class Config:
        case_sensitive = True

# Initialize settings
settings = Settings()

# Set initial modification time
if env_path.exists():
    settings._env_mtime = env_path.stat().st_mtime

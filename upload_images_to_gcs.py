import os
import sys
import asyncio
import aiohttp
import logging
from typing import List, Dict, Optional
from pymongo import MongoClient
from google.cloud import storage
import time
from datetime import datetime
import json

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'upload_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)

# Configurazione da variabili d'ambiente
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
BUCKET_NAME = os.getenv("BUCKET_NAME")
GCS_CREDENTIALS_JSON = os.getenv("GCS_CREDENTIALS_JSON")

# Configurazione rate limiting e performance
MAX_CONCURRENT_DOWNLOADS = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "10"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "2"))

class ImageUploader:
    def __init__(self):
        self.validate_config()
        self.setup_clients()
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'skipped': 0
        }
    
    def validate_config(self):
        """Valida che tutte le configurazioni necessarie siano presenti"""
        required_vars = {
            'MONGO_URI': MONGO_URI,
            'DB_NAME': DB_NAME,
            'COLLECTION_NAME': COLLECTION_NAME,
            'BUCKET_NAME': BUCKET_NAME,
            'GCS_CREDENTIALS_JSON': GCS_CREDENTIALS_JSON
        }
        
        missing = [key for key, value in required_vars.items() if not value]
        if missing:
            logger.error(f"Variabili d'ambiente mancanti: {', '.join(missing)}")
            raise ValueError(f"Configurazione incompleta: {', '.join(missing)}")
    
    def setup_clients(self):
        """Inizializza i client MongoDB e GCS"""
        try:
            # MongoDB
            logger.info("Connessione a MongoDB...")
            self.mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            self.db = self.mongo_client[DB_NAME]
            self.collection = self.db[COLLECTION_NAME]
            
            # Test connessione
            self.mongo_client.server_info()
            logger.info(f"✅ Connesso a MongoDB - Database: {DB_NAME}, Collection: {COLLECTION_NAME}")
            
            # Google Cloud Storage
            logger.info("Connessione a Google Cloud Storage...")
            credentials_path = self.setup_gcs_credentials()
            self.storage_client = storage.Client.from_service_account_json(credentials_path)
            self.bucket = self.storage_client.bucket(BUCKET_NAME)
            
            # Test bucket access
            if not self.bucket.exists():
                raise ValueError(f"Bucket {BUCKET_NAME} non esiste")
            
            logger.info(f"✅ Connesso a GCS - Bucket: {BUCKET_NAME}")
            
        except Exception as e:
            logger.error(f"Errore durante setup dei client: {e}")
            raise
    
    def setup_gcs_credentials(self) -> str:
        """Crea file temporaneo con credenziali GCS dal JSON"""
        credentials_path = "/tmp/gcs_credentials.json" if os.name != 'nt' else "gcs_credentials.json"
        
        try:
            with open(credentials_path, 'w') as f:
                json.dump(json.loads(GCS_CREDENTIALS_JSON), f)
            logger.info("Credenziali GCS configurate")
            return credentials_path
        except json.JSONDecodeError as e:
            logger.error(f"Formato JSON credenziali GCS non valido: {e}")
            raise
    
    async def download_image(self, session: aiohttp.ClientSession, url: str, 
                            product_id: str, media_type: str) -> Optional[Dict]:
        """Scarica un'immagine con retry logic"""
        for attempt in range(MAX_RETRIES):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as response:
                    if response.status == 200:
                        content = await response.read()
                        return {
                            'product_id': product_id,
                            'media_type': media_type,
                            'content': content,
                            'url': url
                        }
                    else:
                        logger.warning(f"Download fallito: {url} (status {response.status})")
                        if attempt < MAX_RETRIES - 1:
                            await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                        
            except asyncio.TimeoutError:
                logger.warning(f"Timeout per {url} (tentativo {attempt + 1}/{MAX_RETRIES})")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    
            except Exception as e:
                logger.error(f"Errore download {url}: {e}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
        
        return None
    
    def upload_to_gcs(self, image_data: Dict) -> bool:
        """Carica un'immagine su GCS"""
        try:
            filename = f"{image_data['product_id']}_{image_data['media_type']}.jpg"
            blob = self.bucket.blob(filename)
            
            # Verifica se esiste già
            if blob.exists():
                logger.info(f"⏭️  Skip (già esistente): {filename}")
                self.stats['skipped'] += 1
                return True
            
            blob.upload_from_string(
                image_data['content'], 
                content_type="image/jpeg",
                timeout=60
            )
            logger.info(f"✅ Caricato: {filename}")
            self.stats['success'] += 1
            return True
            
        except Exception as e:
            logger.error(f"❌ Errore upload {filename}: {e}")
            self.stats['failed'] += 1
            return False
    
    async def process_batch(self, documents: List[Dict]):
        """Processa un batch di documenti con download paralleli"""
        download_tasks = []
        
        async with aiohttp.ClientSession() as session:
            for doc in documents:
                product_id = doc.get("productId")
                if not product_id:
                    continue
                
                for media in doc.get("media", []):
                    url = media.get("medium")
                    media_type = media.get("type", "unknown")
                    
                    if url:
                        self.stats['total'] += 1
                        task = self.download_image(session, url, product_id, media_type)
                        download_tasks.append(task)
            
            # Download paralleli con limite di concorrenza
            logger.info(f"Inizio download di {len(download_tasks)} immagini (max {MAX_CONCURRENT_DOWNLOADS} concurrent)...")
            
            semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
            
            async def download_with_semaphore(task):
                async with semaphore:
                    return await task
            
            results = await asyncio.gather(*[download_with_semaphore(task) for task in download_tasks])
            
            # Upload su GCS (sequenziale per evitare rate limiting)
            logger.info("Inizio upload su GCS...")
            for image_data in results:
                if image_data:
                    self.upload_to_gcs(image_data)
                    # Piccolo delay per evitare rate limiting GCS
                    await asyncio.sleep(0.1)
    
    async def run(self):
        """Esegue il processo completo di upload"""
        try:
            logger.info("=" * 60)
            logger.info("Inizio processo di upload immagini")
            logger.info("=" * 60)
            
            start_time = time.time()
            
            # Recupera tutti i documenti
            logger.info("Recupero documenti da MongoDB...")
            documents = list(self.collection.find())
            logger.info(f"Trovati {len(documents)} documenti")
            
            if not documents:
                logger.warning("Nessun documento trovato nella collection")
                return
            
            # Processa tutti i documenti
            await self.process_batch(documents)
            
            # Statistiche finali
            elapsed_time = time.time() - start_time
            logger.info("=" * 60)
            logger.info("PROCESSO COMPLETATO")
            logger.info(f"Tempo totale: {elapsed_time:.2f} secondi")
            logger.info(f"Immagini totali: {self.stats['total']}")
            logger.info(f"✅ Successi: {self.stats['success']}")
            logger.info(f"⏭️  Skipped: {self.stats['skipped']}")
            logger.info(f"❌ Falliti: {self.stats['failed']}")
            logger.info(f"Velocità media: {self.stats['total']/elapsed_time:.2f} immagini/secondo")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"Errore durante l'esecuzione: {e}")
            raise
        finally:
            if hasattr(self, 'mongo_client'):
                self.mongo_client.close()
                logger.info("Connessione MongoDB chiusa")

def main():
    """Entry point dello script"""
    try:
        uploader = ImageUploader()
        asyncio.run(uploader.run())
    except Exception as e:
        logger.error(f"Errore fatale: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

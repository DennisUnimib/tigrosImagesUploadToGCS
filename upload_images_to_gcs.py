import os
import sys
import json
import logging
import asyncio
import aiohttp
from typing import List, Dict, Optional
from pymongo import MongoClient
from google.cloud import storage
from datetime import datetime
import time

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

# ✨ NUOVE CONFIGURAZIONI BULK
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))  # Documenti per batch
UPLOAD_BATCH_SIZE = int(os.getenv("UPLOAD_BATCH_SIZE", "50"))  # Upload simultanei per batch
DELAY_BETWEEN_BATCHES = float(os.getenv("DELAY_BETWEEN_BATCHES", "1.0"))  # Secondi tra batch

class ImageUploader:
    def __init__(self):
        self.validate_config()
        self.setup_clients()
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'batches_processed': 0
        }
        self.existing_files_cache = set()
        self.start_time = None
    
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
        
        logger.info(f"📊 Configurazione Bulk:")
        logger.info(f"  - Batch size (MongoDB): {BATCH_SIZE}")
        logger.info(f"  - Upload batch size: {UPLOAD_BATCH_SIZE}")
        logger.info(f"  - Concurrent downloads: {MAX_CONCURRENT_DOWNLOADS}")
        logger.info(f"  - Delay between batches: {DELAY_BETWEEN_BATCHES}s")
    
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
            
            # Test upload permessi (invece di bucket.exists())
            try:
                test_blob = self.bucket.blob('.gcs_access_test')
                test_blob.upload_from_string('test', content_type='text/plain')
                test_blob.delete()
                logger.info(f"✅ Connesso a GCS - Bucket: {BUCKET_NAME} (write access verified)")
            except Exception as e:
                raise ValueError(f"Impossibile scrivere nel bucket {BUCKET_NAME}: {e}")
            
        except Exception as e:
            logger.error(f"Errore durante setup dei client: {e}")
            raise
    
    def setup_gcs_credentials(self) -> str:
        """Crea file temporaneo con credenziali GCS dal JSON"""
        credentials_path = "/tmp/gcs_credentials.json" if os.name != 'nt' else "gcs_credentials.json"
        
        try:
            credentials_dict = json.loads(GCS_CREDENTIALS_JSON)
            with open(credentials_path, 'w') as f:
                json.dump(credentials_dict, f)
            logger.info("Credenziali GCS configurate")
            return credentials_path
            
        except json.JSONDecodeError as e:
            logger.error(f"Errore parsing JSON credenziali GCS: {e}")
            raise ValueError("GCS_CREDENTIALS_JSON non è un JSON valido")
    
    def build_existing_files_cache(self):
        """Costruisce cache di file già esistenti su GCS (bulk list)"""
        logger.info("🔍 Costruendo cache file esistenti (bulk list)...")
        
        try:
            start = time.time()
            blobs = self.bucket.list_blobs()
            self.existing_files_cache = {blob.name for blob in blobs}
            elapsed = time.time() - start
            
            logger.info(f"✅ Cache costruita in {elapsed:.2f}s: {len(self.existing_files_cache)} file esistenti")
            
        except Exception as e:
            logger.warning(f"⚠️ Errore costruzione cache: {e}")
            self.existing_files_cache = set()
    
    def file_exists_in_cache(self, filename: str) -> bool:
        """Verifica esistenza file usando cache"""
        return filename in self.existing_files_cache
    
    async def download_image(self, session: aiohttp.ClientSession, url: str, 
                            product_id: str, media_type: str) -> Optional[Dict]:
        """Scarica un'immagine con retry logic"""
        for attempt in range(MAX_RETRIES):
            try:
                async with session.get(url, timeout=REQUEST_TIMEOUT) as response:
                    if response.status == 200:
                        content = await response.read()
                        return {
                            'product_id': product_id,
                            'media_type': media_type,
                            'content': content,
                            'url': url
                        }
                    else:
                        logger.warning(f"⚠️ HTTP {response.status} per {url}")
                        
            except asyncio.TimeoutError:
                logger.warning(f"⏱️ Timeout download {url} (tentativo {attempt + 1}/{MAX_RETRIES})")
            except Exception as e:
                logger.warning(f"❌ Errore download {url}: {e} (tentativo {attempt + 1}/{MAX_RETRIES})")
            
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
        
        return None
    
    def upload_to_gcs_bulk(self, images_data: List[Dict]) -> Dict[str, int]:
        """Carica multiple immagini su GCS in batch"""
        results = {'success': 0, 'failed': 0, 'skipped': 0}
        
        for image_data in images_data:
            try:
                filename = f"{image_data['product_id']}_{image_data['media_type']}.jpg"
                
                # Controllo cache
                if self.file_exists_in_cache(filename):
                    logger.debug(f"⏭️ Cache-skip: {filename}")
                    results['skipped'] += 1
                    continue
                
                blob = self.bucket.blob(filename)
                
                # Doppio controllo GCS (per sicurezza)
                if blob.exists():
                    logger.debug(f"⏭️ GCS-skip: {filename}")
                    self.existing_files_cache.add(filename)
                    results['skipped'] += 1
                    continue
                
                # Upload
                blob.upload_from_string(
                    image_data['content'], 
                    content_type="image/jpeg",
                    timeout=60
                )
                
                # Aggiorna cache
                self.existing_files_cache.add(filename)
                results['success'] += 1
                logger.debug(f"✅ Uploaded: {filename}")
                
                # Delay minimo tra upload per non sovraccaricare
                time.sleep(0.05)
                
            except Exception as e:
                logger.error(f"❌ Errore upload {filename}: {e}")
                results['failed'] += 1
        
        return results
    
    async def process_batch(self, documents: List[Dict], batch_num: int, total_batches: int):
        """Processa un batch di documenti con download e upload paralleli"""
        logger.info(f"📦 Processando batch {batch_num}/{total_batches} ({len(documents)} documenti)...")
        
        # Raccolta URL da scaricare (filtrando già esistenti)
        download_tasks = []
        
        async with aiohttp.ClientSession() as session:
            semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
            
            for doc in documents:
                product_id = doc.get('productId', 'unknown')
                
                if 'media' in doc and isinstance(doc['media'], list):
                    for media_item in doc['media']:
                        if 'medium' in media_item and 'type' in media_item:
                            filename = f"{product_id}_{media_item['type']}.jpg"
                            
                            # Skip se in cache
                            if self.file_exists_in_cache(filename):
                                logger.debug(f"⏭️ Pre-skip: {filename}")
                                self.stats['skipped'] += 1
                                continue
                            
                            # Crea task download con semaphore
                            async def download_with_semaphore(url, pid, mtype):
                                async with semaphore:
                                    return await self.download_image(session, url, pid, mtype)
                            
                            download_tasks.append(
                                download_with_semaphore(media_item['medium'], product_id, media_item['type'])
                            )
            
            if not download_tasks:
                logger.info(f"✅ Batch {batch_num}: Nessuna nuova immagine da scaricare")
                return
            
            # Download parallelo
            logger.info(f"📥 Downloading {len(download_tasks)} immagini...")
            download_start = time.time()
            downloaded_images = await asyncio.gather(*download_tasks)
            download_time = time.time() - download_start
            
            # Filtra risultati None (falliti)
            valid_images = [img for img in downloaded_images if img is not None]
            
            logger.info(f"✅ Downloaded {len(valid_images)}/{len(download_tasks)} in {download_time:.2f}s")
            
            if not valid_images:
                logger.warning(f"⚠️ Batch {batch_num}: Nessuna immagine scaricata con successo")
                self.stats['failed'] += len(download_tasks)
                return
        
        # Upload bulk (sincrono, ma in batch)
        logger.info(f"☁️ Uploading {len(valid_images)} immagini a GCS...")
        upload_start = time.time()
        
        # Dividi in sub-batch per upload
        upload_results = {'success': 0, 'failed': 0, 'skipped': 0}
        
        for i in range(0, len(valid_images), UPLOAD_BATCH_SIZE):
            sub_batch = valid_images[i:i + UPLOAD_BATCH_SIZE]
            results = self.upload_to_gcs_bulk(sub_batch)
            
            upload_results['success'] += results['success']
            upload_results['failed'] += results['failed']
            upload_results['skipped'] += results['skipped']
            
            logger.info(f"  Sub-batch {i//UPLOAD_BATCH_SIZE + 1}: "
                       f"✅ {results['success']} | ⏭️ {results['skipped']} | ❌ {results['failed']}")
        
        upload_time = time.time() - upload_start
        
        # Aggiorna statistiche globali
        self.stats['success'] += upload_results['success']
        self.stats['failed'] += upload_results['failed'] + (len(download_tasks) - len(valid_images))
        self.stats['skipped'] += upload_results['skipped']
        
        logger.info(f"✅ Batch {batch_num} completato in {upload_time:.2f}s: "
                   f"✅ {upload_results['success']} | ⏭️ {upload_results['skipped']} | ❌ {upload_results['failed']}")
    
    async def run(self):
        """Esegue il processo completo di upload con logica bulk"""
        try:
            self.start_time = time.time()
            logger.info("🚀 Inizio processo upload immagini (BULK MODE)")
            
            # Costruisci cache file esistenti (operazione bulk)
            self.build_existing_files_cache()
            
            # Conta documenti totali
            total_docs = self.collection.count_documents({})
            logger.info(f"📊 Documenti totali da processare: {total_docs}")
            
            if total_docs == 0:
                logger.warning("⚠️ Nessun documento trovato nella collection")
                return
            
            self.stats['total'] = total_docs
            
            # Calcola numero di batch
            total_batches = (total_docs + BATCH_SIZE - 1) // BATCH_SIZE
            logger.info(f"📦 Suddiviso in {total_batches} batch da {BATCH_SIZE} documenti")
            
            # Processa batch
            for batch_num in range(1, total_batches + 1):
                skip = (batch_num - 1) * BATCH_SIZE
                
                # Fetch batch da MongoDB
                batch_docs = list(self.collection.find().skip(skip).limit(BATCH_SIZE))
                
                if not batch_docs:
                    logger.warning(f"⚠️ Batch {batch_num} vuoto, skip")
                    continue
                
                # Processa batch
                await self.process_batch(batch_docs, batch_num, total_batches)
                
                self.stats['batches_processed'] += 1
                
                # Delay tra batch (per non sovraccaricare)
                if batch_num < total_batches:
                    logger.info(f"⏸️ Pausa {DELAY_BETWEEN_BATCHES}s prima del prossimo batch...")
                    await asyncio.sleep(DELAY_BETWEEN_BATCHES)
            
            # Statistiche finali
            elapsed_time = time.time() - self.start_time
            
            logger.info("=" * 60)
            logger.info("📊 STATISTICHE FINALI")
            logger.info("=" * 60)
            logger.info(f"📦 Batch processati: {self.stats['batches_processed']}/{total_batches}")
            logger.info(f"📄 Documenti totali: {self.stats['total']}")
            logger.info(f"✅ Immagini caricate: {self.stats['success']}")
            logger.info(f"⏭️ Immagini già esistenti: {self.stats['skipped']}")
            logger.info(f"❌ Errori: {self.stats['failed']}")
            logger.info(f"⏱️ Tempo totale: {elapsed_time:.2f}s ({elapsed_time/60:.2f} min)")
            
            if self.stats['success'] > 0:
                avg_speed = self.stats['success'] / elapsed_time
                logger.info(f"⚡ Velocità media: {avg_speed:.2f} immagini/secondo")
            
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"❌ Errore durante esecuzione: {e}")
            raise
        finally:
            # Cleanup
            if hasattr(self, 'mongo_client'):
                self.mongo_client.close()
                logger.info("🔌 Connessione MongoDB chiusa")

def main():
    """Entry point dello script"""
    try:
        uploader = ImageUploader()
        asyncio.run(uploader.run())
    except Exception as e:
        logger.error(f"💥 Errore fatale: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

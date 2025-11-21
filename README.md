# Tigros Images Upload to Google Cloud Storage

Script automatizzato per scaricare immagini di prodotti da MongoDB e caricarle su Google Cloud Storage (GCS) con GitHub Actions.

## 🚀 Caratteristiche

- 🔒 **Sicurezza**: Configurazione tramite GitHub Secrets (zero credenziali hardcoded)
- ⚡ **Performance**: Download asincroni paralleli con rate limiting configurabile
- 🔄 **Affidabilità**: Retry automatico su errori, timeout configurabili
- 📊 **Monitoring**: Logging dettagliato e statistiche finali
- 🤖 **Automazione**: Esecuzione schedulata giornaliera automatica
- 🎯 **Ottimizzazione**: Skip automatico di immagini già esistenti

## 📋 Prerequisiti

- Repository GitHub
- Account MongoDB Atlas
- Google Cloud Storage bucket
- Service Account GCS con permessi di scrittura

## 🔐 Setup GitHub Secrets

### 1. Configura i Secrets Richiesti

Vai su **Settings → Secrets and variables → Actions → New repository secret**

#### Credenziali (Sensibili)

**`MONGO_URI`** - Connection string MongoDB Atlas
```
mongodb+srv://username:password@cluster.mongodb.net/?retryWrites=true&w=majority
```

**`GCS_CREDENTIALS_JSON`** - Service Account JSON completo
```json
{
  "type": "service_account",
  "project_id": "your-project-id",
  "private_key_id": "abc123...",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
  "client_email": "service-account@project.iam.gserviceaccount.com",
  "client_id": "123456789",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/..."
}
```

#### Configurazione

**`DB_NAME`** - Nome database MongoDB
```
tigros
```

**`COLLECTION_NAME`** - Nome collection MongoDB
```
images-test
```

**`BUCKET_NAME`** - Nome bucket Google Cloud Storage
```
images_product_tigros
```

### 2. Verifica Configurazione

Lo script validerà automaticamente tutti i secrets all'avvio. In caso di errori, controlla i logs su GitHub Actions.

## 🚀 Esecuzione su GitHub Actions

### Modalità di Esecuzione

#### 1. Automatica (Scheduled)
- Esecuzione giornaliera automatica alle **2:00 AM UTC**
- Configurata nel workflow, non richiede intervento

#### 2. Manuale (Workflow Dispatch)
1. Vai al tab **Actions**
2. Seleziona **Upload Tigros Images to GCS**
3. Click su **Run workflow**
4. (Opzionale) Configura parametri:
   - **Max concurrent downloads**: Numero di download simultanei (default: 10)
   - **Collection name**: Override temporaneo del nome collection
5. Click su **Run workflow** per confermare

### 📊 Monitoraggio

#### Logs in Tempo Reale
- **Actions** → Seleziona workflow run → Click sul job **upload-images**
- Visualizza output in tempo reale durante l'esecuzione

#### Logs Scaricabili
- Ogni esecuzione salva i log come **artifacts**
- Download da: **Actions** → [Run specifica] → Sezione **Artifacts**
- Retention: 30 giorni

#### Statistiche
Ogni esecuzione mostra:
```
============================================================
PROCESSO COMPLETATO
Tempo totale: 45.32 secondi
Immagini totali: 1250
✅ Successi: 1180
⏭️  Skipped: 50
❌ Falliti: 20
Velocità media: 27.58 immagini/secondo
============================================================
```

## 🔧 Ottimizzazioni Implementate

### Performance
- **Download asincroni**: Fino a 10 download simultanei (configurabile)
- **Semaphore pattern**: Controllo concorrenza per evitare sovraccarico
- **Skip duplicati**: Verifica esistenza su GCS prima dell'upload
- **Connection pooling**: Riutilizzo connessioni HTTP

### Affidabilità
- **Retry logic**: 3 tentativi con backoff esponenziale
- **Timeout configurabili**: Evita blocchi infiniti (30s HTTP, 60s GCS)
- **Error handling**: Gestione granulare degli errori
- **Graceful shutdown**: Chiusura corretta connessioni

### Sicurezza
- **GitHub Secrets**: Tutte le credenziali gestite in modo sicuro
- **Zero hardcoding**: Nessuna credenziale nel codice
- **File temporanei**: Credenziali GCS in `/tmp` (cancellate dopo uso)
- **Validation**: Controllo configurazione all'avvio
- **Gitignore**: Protezione file sensibili

## 🔍 Troubleshooting

### ❌ Errore: "Variabili d'ambiente mancanti"
**Soluzione**: Verifica che tutti i secrets richiesti siano configurati su GitHub:
- `MONGO_URI`
- `GCS_CREDENTIALS_JSON`
- `DB_NAME`
- `COLLECTION_NAME`
- `BUCKET_NAME`

### ⏱️ Timeout durante download
**Soluzione**: Riduci `max_concurrent_downloads` durante l'esecuzione manuale (es. da 10 a 5)

### 🚫 Rate limiting GCS
**Soluzione**: Lo script include già un delay di 0.1s tra upload. Se persiste, riduci ulteriormente i download simultanei.

### 🔌 MongoDB connection timeout
**Soluzione**: Verifica che:
1. L'URI in `MONGO_URI` sia corretto
2. Il cluster MongoDB sia attivo
3. Le credenziali siano valide
4. **IMPORTANTE**: L'IP di GitHub Actions sia whitelisted su MongoDB Atlas
   - Vai su MongoDB Atlas → Network Access
   - Aggiungi IP: `0.0.0.0/0` (tutti gli IP) per GitHub Actions

### 🔑 GCS Authentication Error
**Soluzione**: Verifica che:
1. Il JSON in `GCS_CREDENTIALS_JSON` sia completo e valido
2. Il service account abbia i permessi corretti sul bucket
3. Il bucket esista e il nome sia corretto

## 🛠️ Test Locale (Opzionale)

⚠️ **Non raccomandato** - preferisci usare GitHub Actions direttamente.

Se necessario testare localmente:

### Windows PowerShell
```powershell
# Imposta variabili d'ambiente
$env:MONGO_URI="mongodb+srv://username:password@cluster.mongodb.net/"
$env:GCS_CREDENTIALS_JSON='{"type":"service_account","project_id":"...","private_key":"...",...}'
$env:DB_NAME="tigros"
$env:COLLECTION_NAME="images-test"
$env:BUCKET_NAME="images_product_tigros"

# Installa dipendenze
pip install -r requirements.txt

# Esegui script
python importimmaginicloud.py
```

### Linux/Mac
```bash
# Imposta variabili d'ambiente
export MONGO_URI="mongodb+srv://username:password@cluster.mongodb.net/"
export GCS_CREDENTIALS_JSON='{"type":"service_account","project_id":"...","private_key":"...",...}'
export DB_NAME="tigros"
export COLLECTION_NAME="images-test"
export BUCKET_NAME="images_product_tigros"

# Installa dipendenze
pip install -r requirements.txt

# Esegui script
python importimmaginicloud.py
```

⚠️ **Attenzione**: Non committare mai credenziali! Usa solo per test temporanei.

## 📁 Struttura Progetto

```
tigrosImagesUploadToGCS/
├── .github/
│   └── workflows/
│       └── upload-images.yml    # GitHub Actions workflow
├── importimmaginicloud.py       # Script principale
├── requirements.txt             # Dipendenze Python
├── .env.example                 # Template riferimento (non usare direttamente)
├── .gitignore                   # File da ignorare
└── README.md                    # Questa documentazione
```

## 📝 Note Tecniche

- **Naming immagini**: `{productId}_{mediaType}.jpg`
- **Formato log**: `upload_YYYYMMDD_HHMMSS.log`
- **Timeout workflow**: 2 ore (configurabile in `.github/workflows/upload-images.yml`)
- **Retention artifacts**: 30 giorni
- **Python version**: 3.11
- **Dipendenze**: Vedi `requirements.txt`

## 🔒 Checklist Sicurezza

Prima di committare, verifica:
- [ ] Nessun file `.env` nel repository
- [ ] Nessuna credenziale hardcoded nel codice
- [ ] `.gitignore` include `.env`, `*.json`, `gcs_credentials.json`
- [ ] Tutti i secrets configurati su GitHub
- [ ] Service account GCS ha permessi minimi necessari
- [ ] MongoDB Network Access configurato correttamente

## 📄 Licenza

[Specifica la licenza del progetto]

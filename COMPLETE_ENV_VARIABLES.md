# Complete Environment Variables for Railway Backend API

## 🚨 CRITICAL - REQUIRED VARIABLES

These **MUST** be set for the system to function. No safe defaults exist.

```env
# =============================================================================
# REQUIRED: CORE INFRASTRUCTURE
# =============================================================================

# OpenAI API Key (REQUIRED)
# Get from: https://platform.openai.com/api-keys
OPENAI_API_KEY=sk-proj-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# Supabase Database Configuration (REQUIRED)
# Get from: https://supabase.com/dashboard/project/YOUR_PROJECT/settings/api
SUPABASE_URL=https://xxxxxxxxxxxxx.supabase.co
SUPABASE_ANON_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.xxxxxxxxxxxxx

# Redis Configuration (REQUIRED for async job processing)
# Railway provides REDIS_URL automatically when you add Redis service
# Format: redis://default:password@host:port
REDIS_URL=redis://default:xxxxxxxxxxxxx@containers-us-west-xxx.railway.app:xxxxx

# Environment (REQUIRED)
ENVIRONMENT=production

# CORS Configuration (REQUIRED for frontend access)
# Your Netlify frontend URL
ALLOWED_ORIGINS=https://imtehaanai.netlify.app,https://imtehaanai.netlify.app/*
ALLOW_CREDENTIALS=true
```

---

## ⚙️ SERVER CONFIGURATION

```env
# =============================================================================
# API SERVER CONFIGURATION
# =============================================================================

# Server binding (Railway sets PORT automatically)
API_HOST=0.0.0.0
API_PORT=8000
# Note: Railway provides PORT automatically - API_PORT is fallback

# Uvicorn Configuration (Production-Grade)
UVICORN_TIMEOUT_KEEP_ALIVE=30
UVICORN_TIMEOUT_GRACEFUL_SHUTDOWN=10
UVICORN_LIMIT_CONCURRENCY=1000
UVICORN_BACKLOG=2048
UVICORN_WORKERS=1

# Request Timeout (for Redis/DB operations)
REQUEST_TIMEOUT=5
```

---

## 🔒 SECURITY & SAFETY

```env
# =============================================================================
# SECURITY CONFIGURATION
# =============================================================================

# Safety Guardrail: Prevent dev/staging from using production Supabase
ALLOW_PROD_SUPABASE_IN_DEV=false

# Optional: Service role key for admin operations (higher privileges)
# Only use in secure environments, never expose to frontend
SUPABASE_SERVICE_ROLE_KEY=

# Load Shedding (Prevents system overload)
LOAD_SHEDDING_ENABLED=true
LOAD_SHEDDING_QUEUE_THRESHOLD=0.9
LOAD_SHEDDING_WORKER_DEGRADED_THRESHOLD=2

# Safety Gate (Centralized safety checks)
SAFETY_GATE_ENABLED=true
SAFETY_GATE_QUEUE_THRESHOLD=0.9
SAFETY_GATE_MEMORY_THRESHOLD_MB=400
SAFETY_GATE_MEMORY_PERCENT_THRESHOLD=80
SAFETY_GATE_MIN_HEALTHY_WORKERS=2
```

---

## 🤖 AI MODEL CONFIGURATION

```env
# =============================================================================
# AI MODEL CONFIGURATION
# =============================================================================

# Tutor Agent Model
TUTOR_MODEL=gpt-4o-mini
TUTOR_TEMPERATURE=1.0
TUTOR_MAX_TOKENS=2000

# Answer Grading Agent Model
GRADING_MODEL=gpt-4o-mini
GRADING_TEMPERATURE=0.1
GRADING_MAX_TOKENS=2000

# Helping Agent Model (concept explanations)
HELPING_MODEL=gpt-4o-mini
HELPING_TEMPERATURE=0.3
HELPING_MAX_TOKENS=150

# Helping Agent Model Selection (intelligent routing)
HELPING_ENABLE_MODEL_SELECTION=true
HELPING_FAST_MODEL=gpt-3.5-turbo
HELPING_MIN_CONFIDENCE_FAST=0.7
```

---

## 📊 OBSERVABILITY & LOGGING

```env
# =============================================================================
# OBSERVABILITY AND LOGGING
# =============================================================================

# Log Level: DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_LEVEL=INFO

# Log Format: json (structured, production) or text (human-readable)
LOG_FORMAT=json

# Enable Debug Mode (boolean)
ENABLE_DEBUG=false

# LangSmith Tracing Configuration (Optional)
LANGSMITH_TRACING=false
LANGSMITH_API_KEY=
LANGSMITH_PROJECT=imtehaan-ai-tutor
LANGSMITH_ENDPOINT=https://api.smith.langchain.com
```

---

## 🔄 REDIS CONFIGURATION

```env
# =============================================================================
# REDIS CONNECTION RETRY CONFIGURATION
# =============================================================================

REDIS_RETRY_BASE_DELAY=2.0
REDIS_RETRY_MAX_DELAY=60.0
REDIS_RETRY_MAX_ATTEMPTS=0
REDIS_HEALTH_CHECK_INTERVAL=10
```

---

## 📦 JOB QUEUE CONFIGURATION

```env
# =============================================================================
# REDIS-BACKED JOB QUEUE CONFIGURATION
# =============================================================================

# Job Result Retention (seconds)
JOB_RESULT_TTL=86400

# Job Retry Configuration
MAX_RETRIES=3
RETRY_DELAY=60
RETRY_EXPONENTIAL_BACKOFF=true
MAX_RETRY_DELAY=600

# Job Timeout (seconds)
JOB_TIMEOUT=3600
JOB_TIMEOUT_TUTOR_CHAT=300
JOB_TIMEOUT_WARNING=1800
STRICT_TIMEOUT_ENFORCEMENT=true

# Idempotency Configuration
ENABLE_IDEMPOTENCY=true
IDEMPOTENCY_WINDOW=3600
```

---

## 🛡️ BACK-PRESSURE MECHANISMS

```env
# =============================================================================
# BACK-PRESSURE MECHANISMS
# =============================================================================

# Maximum Queue Size
MAX_QUEUE_SIZE=10000

# Queue Full Policy: reject (recommended) or drop_oldest
QUEUE_FULL_POLICY=reject

# Queue Back-Pressure Threshold
QUEUE_BACK_PRESSURE_THRESHOLD=0.8
QUEUE_BACK_PRESSURE_DELAY=1.0
```

---

## 👷 WORKER CONFIGURATION

```env
# =============================================================================
# WORKER CONFIGURATION
# =============================================================================

# Worker Concurrency: Maximum concurrent jobs per worker process
WORKER_CONCURRENCY=3

# Database Connection Pool Size
MAX_DB_CONNECTIONS=5

# Worker Poll Timeout (seconds)
WORKER_POLL_TIMEOUT=5

# Worker Max Jobs Per Loop
WORKER_MAX_JOBS_PER_LOOP=1
```

---

## 🗄️ DATABASE CONFIGURATION

```env
# =============================================================================
# DATABASE LOAD SAFETY: OPERATION TIMEOUTS & RETRIES
# =============================================================================

DB_OPERATION_TIMEOUT=30.0
DB_MAX_RETRIES=3
DB_RETRY_BASE_DELAY=1.0
DB_RETRY_MAX_DELAY=10.0

# Query Pagination
DB_MAX_PAGE_SIZE=50
DB_ABSOLUTE_MAX_PAGE_SIZE=1000

# Worker Throttling
WORKER_DB_ERROR_THRESHOLD=5
WORKER_DB_ERROR_PAUSE_MIN=30
WORKER_DB_ERROR_PAUSE_MAX=60

# Per-Job-Type Rate Limits (jobs per minute, 0 = no limit)
WORKER_RATE_LIMIT_GRADING=10
WORKER_RATE_LIMIT_ANALYTICS=5
WORKER_RATE_LIMIT_GENERATION=3
WORKER_RATE_LIMIT_ROLLUP=20

# Supabase Max Concurrency
SUPABASE_MAX_CONCURRENCY=5
SUPABASE_MAX_CONCURRENCY_WORKER=2

# Batch Parallelization Concurrency Multipliers
BATCH_MASTERY_UPDATE_CONCURRENCY_MULTIPLIER=2.0
BATCH_CONCEPT_PROCESSING_CONCURRENCY_MULTIPLIER=2.0
BATCH_DEFAULT_CONCURRENCY_MULTIPLIER=1.5
```

---

## 🎯 WORKLOAD ISOLATION

```env
# =============================================================================
# WORKLOAD ISOLATION: JOB-TYPE SPECIFIC CONCURRENCY LIMITS
# =============================================================================

JOB_CONCURRENCY_TUTOR_CHAT=2
JOB_CONCURRENCY_GRADE_ANSWER=4
JOB_CONCURRENCY_GRADE_MOCK_EXAM=1
JOB_CONCURRENCY_EXPLAIN_CONCEPT=5
JOB_CONCURRENCY_CREATE_LESSON=3
```

---

## ⚡ AI PROVIDER RATE LIMIT PROTECTION

```env
# =============================================================================
# AI PROVIDER RATE LIMIT PROTECTION
# =============================================================================

# OpenAI Rate Limits (adjust based on your OpenAI plan)
OPENAI_RATE_LIMIT_RPM=500
OPENAI_RATE_LIMIT_TPM=200000
```

---

## 🚦 RATE LIMITING (PERMANENT ENFORCEMENT)

```env
# =============================================================================
# RATE LIMITING CONFIGURATION (PERMANENT ENFORCEMENT)
# =============================================================================

# Tutor Chat Rate Limits (requests per window)
RATE_LIMIT_TUTOR_CHAT_REQUESTS=60
RATE_LIMIT_TUTOR_CHAT_WINDOW=3600

# Answer Grading Rate Limits
RATE_LIMIT_ANSWER_GRADING_REQUESTS=100
RATE_LIMIT_ANSWER_GRADING_WINDOW=3600

# Mock Exam Grading Rate Limits
RATE_LIMIT_MOCK_EXAM_GRADING_REQUESTS=10
RATE_LIMIT_MOCK_EXAM_GRADING_WINDOW=3600

# Concept Explanation Rate Limits
RATE_LIMIT_CONCEPT_EXPLANATION_REQUESTS=200
RATE_LIMIT_CONCEPT_EXPLANATION_WINDOW=3600

# Lesson Creation Rate Limits
RATE_LIMIT_LESSON_CREATION_REQUESTS=30
RATE_LIMIT_LESSON_CREATION_WINDOW=3600

# Global AI Work Rate Limit
RATE_LIMIT_ALL_AI_WORK_REQUESTS=500
RATE_LIMIT_ALL_AI_WORK_WINDOW=3600
```

---

## 🔄 JOB STARVATION PREVENTION

```env
# =============================================================================
# JOB STARVATION PREVENTION
# =============================================================================

SHORT_JOB_MAX_WAIT_SECONDS=30
SHORT_JOB_DURATION_THRESHOLD=5
```

---

## 🏥 HEALTH MONITORING

```env
# =============================================================================
# HEALTH MONITORING AND GRACEFUL DEGRADATION
# =============================================================================

HEALTH_CHECK_INTERVAL=30
WORKER_HEALTH_TTL=60
WORKER_HEALTH_UPDATE_INTERVAL=30
MAX_CONSECUTIVE_FAILURES=10
CIRCUIT_BREAKER_RESET_TIME=300
ENABLE_GRACEFUL_DEGRADATION=true
DEGRADATION_MODE_THRESHOLD=5
```

---

## 💾 CACHING CONFIGURATION

```env
# =============================================================================
# DATABASE OPTIMIZATION: CACHING
# =============================================================================

# Database Cache TTL (seconds)
DB_CACHE_TTL=300
MAX_CACHE_SIZE=1000

# Read-Through Cache TTLs (seconds)
DEFAULT_CACHE_TTL=3600
CACHE_TTL_STATIC=86400
CACHE_TTL_SEMI_STATIC=3600
CACHE_TTL_FREQUENT=300
CACHE_TTL_USER=1800
CACHE_TTL_QUERY=600

# Deterministic Operation Cache TTLs
CACHE_TTL_READINESS_ASSESSMENT=900
```

---

## 🔍 EMBEDDING PRE-GENERATION

```env
# =============================================================================
# EMBEDDING PRE-GENERATION CONFIGURATION
# =============================================================================

ENABLE_EMBEDDING_PREGEN=true
EMBEDDING_PREGEN_INTERVAL_SECONDS=3600
EMBEDDING_PREGEN_BATCH_SIZE=50
EMBEDDING_PREGEN_RATE_LIMIT_DELAY=0.1
EMBEDDING_PREGEN_CACHE_TTL=604800
EMBEDDING_TRACKING_WINDOW_HOURS=24
EMBEDDING_MIN_ACCESS_COUNT=10
```

---

## 📡 STREAMING CONFIGURATION

```env
# =============================================================================
# STREAMING CONFIGURATION
# =============================================================================

STREAMING_ENABLED=true
STREAM_CHUNK_TTL=3600
STREAM_TIMEOUT=300
```

---

## 📝 DATABASE BATCHED WRITES

```env
# =============================================================================
# DATABASE OPTIMIZATION: BATCHED WRITES
# =============================================================================

DB_BATCH_SIZE=50
DB_BATCH_INTERVAL=2.0
MAX_BATCH_WAIT=5.0
```

---

## 💻 MEMORY SAFETY

```env
# =============================================================================
# MEMORY SAFETY CONFIGURATION
# =============================================================================

MEMORY_THRESHOLD_MB=500
MEMORY_MONITORING_ENABLED=true
MEMORY_CHECK_INTERVAL=300
```

---

## 📱 FRONTEND CONFIGURATION

```env
# =============================================================================
# FRONTEND CONFIGURATION (Client-Side)
# =============================================================================

# Dashboard Totals Refresh Interval (seconds)
VITE_DASHBOARD_TOTALS_REFRESH_INTERVAL=90
```

---

## 📋 QUICK SETUP FOR RAILWAY

### Minimum Required Variables (Copy-Paste Ready)

```env
OPENAI_API_KEY=sk-proj-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
SUPABASE_URL=https://xxxxxxxxxxxxx.supabase.co
SUPABASE_ANON_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.xxxxxxxxxxxxx
REDIS_URL=redis://default:xxxxxxxxxxxxx@containers-us-west-xxx.railway.app:xxxxx
ENVIRONMENT=production
ALLOWED_ORIGINS=https://imtehaanai.netlify.app,https://imtehaanai.netlify.app/*
ALLOW_CREDENTIALS=true
```

### Production-Ready Full Configuration

Use the values above for all sections. The defaults in `config.env.example` are production-ready, so you can:

1. **Copy the minimum required variables** above
2. **Add optional variables** only if you need to customize behavior
3. **Railway will use defaults** from the code for all other variables

---

## 🔍 How to Set in Railway

1. Go to your Railway project
2. Select your service (API server)
3. Click on **"Variables"** tab
4. Click **"New Variable"**
5. Add each variable one by one, or use Railway's bulk import feature

---

## ✅ Verification Checklist

After setting environment variables, verify:

- [ ] `OPENAI_API_KEY` is set and valid
- [ ] `SUPABASE_URL` and `SUPABASE_ANON_KEY` are set
- [ ] `REDIS_URL` is set (Railway provides this automatically)
- [ ] `ENVIRONMENT=production`
- [ ] `ALLOWED_ORIGINS` includes your Netlify URL
- [ ] All other variables use defaults (or customize as needed)

---

## 📚 Additional Resources

- **Full Configuration Template**: See `config.env.example` in this repository
- **Deployment Guide**: See `README.md`
- **CORS Configuration**: See `NETLIFY_FRONTEND_URL.md`

---

**Note:** Railway automatically provides `PORT` environment variable. Your code will use it automatically - no need to set it manually.

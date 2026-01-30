# Worker Deployment Guide for Railway

## 🔍 Worker Architecture

The backend uses a **separate worker architecture** where:
- **API Server** (`unified_backend.py`) - Handles HTTP requests, enqueues jobs to Redis
- **Workers** (`workers/enhanced_worker.py`) - Process AI jobs from Redis queues asynchronously

## ❓ Do Workers Need Separate Deployment?

### Answer: **YES, workers need separate Railway services**

Railway runs **one process per service**. Since workers are separate processes, you have two options:

### Option 1: Separate Services (Recommended for Production)

Deploy workers as **separate Railway services**:

1. **Main API Service** (already configured)
   - Runs: `python start_unified_backend.py`
   - Handles: HTTP requests, API endpoints
   - Procfile: `web: python start_unified_backend.py`

2. **Worker Service** (needs separate deployment)
   - Runs: `python workers/enhanced_worker.py`
   - Handles: AI job processing from Redis queues
   - Procfile: `worker: python workers/enhanced_worker.py`

**Benefits:**
- ✅ Independent scaling
- ✅ Better resource isolation
- ✅ Separate monitoring and logs
- ✅ Can restart workers without affecting API

### Option 2: Single Service with Multiple Processes (Not Recommended)

Railway doesn't natively support multiple processes in one service. You could use a process manager, but this is **not recommended** because:
- ❌ No independent scaling
- ❌ Shared resources (memory, CPU)
- ❌ One failure affects both
- ❌ Harder to monitor

## 📋 Deployment Steps for Workers

### Step 1: Create Worker Service in Railway

1. In Railway Dashboard, click **"New"** → **"Service"**
2. Select **"GitHub Repo"** or **"Empty Service"**
3. If using GitHub:
   - Select same repository
   - Set root directory to `railway-deploy`
   - Railway will detect it's Python

### Step 2: Configure Worker Service

**Create `Procfile.worker` in railway-deploy folder:**
```
worker: python workers/enhanced_worker.py
```

**Or set start command in Railway:**
```
python workers/enhanced_worker.py
```

### Step 3: Set Environment Variables

Worker service needs **same environment variables** as API service:
- `OPENAI_API_KEY`
- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`
- `REDIS_URL` (same Redis instance as API)
- `ENVIRONMENT=production`
- All other config variables

### Step 4: Deploy Worker Service

Railway will:
1. Install dependencies from `requirements.txt`
2. Run the worker process
3. Connect to Redis and start processing jobs

## 🔧 Worker Configuration

### Worker Types

You can deploy different worker configurations:

**1. All Queues Worker (Recommended for start)**
```bash
python workers/enhanced_worker.py
```
Processes all job types (tutor, grading, helping, etc.)

**2. Dedicated Queue Workers (For scaling)**
```bash
# Tutor-only worker
python workers/enhanced_worker.py --queues tutor

# Grading-only worker
python workers/enhanced_worker.py --queues grading
```

### Scaling Workers

- Start with **1 worker** processing all queues
- Add more workers as traffic increases
- Scale based on queue depth and processing time

## 📊 Monitoring Workers

### Health Checks

Workers report health to Redis. Check worker health via API:
```
GET /observability/worker-health
```

### Logs

View worker logs in Railway Dashboard → Worker Service → Logs

### Metrics

Monitor:
- Jobs processed per minute
- Queue depth
- Worker health status
- Error rates

## ⚠️ Important Notes

### Redis Connection

- **Both API and Workers must use the SAME Redis instance**
- Set `REDIS_URL` to the same value in both services
- Railway Redis service provides `REDIS_URL` automatically

### Worker Health

- Workers must be healthy for AI jobs to process
- API will reject requests if no healthy workers (load shedding)
- Minimum 2 healthy workers recommended for production

### Resource Limits

- Railway Free Tier: 512MB RAM, $5 credit
- Each worker uses ~200-400MB RAM
- Start with 1 worker, scale as needed

## 🚀 Quick Start

### Minimal Deployment (1 Worker)

1. Deploy API service (already configured)
2. Create worker service:
   - Same repo, same directory
   - Start command: `python workers/enhanced_worker.py`
   - Same environment variables
3. Both services connect to same Redis

### Production Deployment (Multiple Workers)

1. Deploy API service
2. Deploy 2-3 worker services:
   - Worker 1: All queues
   - Worker 2: Tutor queue only
   - Worker 3: Grading queue only
3. Monitor and scale based on load

## 📝 Checklist

- [ ] API service deployed and running
- [ ] Redis service added and `REDIS_URL` set
- [ ] Worker service created
- [ ] Worker service environment variables set (same as API)
- [ ] Worker service start command configured
- [ ] Worker service deployed
- [ ] Worker health check passing
- [ ] Jobs processing successfully

## 🔗 Related Files

- `workers/enhanced_worker.py` - Main worker implementation
- `workers/ai_worker.py` - Alternative worker (legacy)
- `services/job_queue.py` - Job queue management
- `services/redis_connection.py` - Redis connection

---

**Summary:** Workers **MUST** be deployed as a **separate Railway service**. They cannot run in the same service as the API server.

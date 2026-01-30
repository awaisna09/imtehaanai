# Railway API Server Deployment

This repository contains **only the API server** for the Imtehaan AI EdTech Platform. This is deployed as a **separate Railway service** from the workers.

## 📁 What's Included

This repository contains:
- ✅ `unified_backend.py` - Main FastAPI application
- ✅ `start_unified_backend.py` - Startup script
- ✅ `requirements.txt` - Python dependencies
- ✅ `config.env.example` - Environment variables template
- ✅ `cache.py` - Caching utilities
- ✅ `study_planner_service.py` - Study planner service
- ✅ `langgraph_tutor.py` - LangGraph tutor implementation
- ✅ `agents/` - AI agent implementations
- ✅ `services/` - Backend services (Supabase, Redis, rate limiting, etc.)

**Note:** This repository does NOT include:
- ❌ `workers/` directory (workers - separate repository)
- ❌ Worker-specific files

## 🚀 Deployment Steps

### Step 1: Create Railway Project

1. Go to [Railway Dashboard](https://railway.app)
2. Click "New Project"
3. Select "Deploy from GitHub repo"
4. Connect this repository (API server-only repository)
5. Railway will auto-detect Python and build

### Step 2: Configure Build Settings

Railway will automatically:
- Detect Python from `runtime.txt`
- Install dependencies from `requirements.txt`
- Run the command from `Procfile`: `python start_unified_backend.py`

### Step 3: Set Environment Variables

In Railway Dashboard → Project → Variables, add:

#### Required Variables

```
# OpenAI API Key
OPENAI_API_KEY=your_openai_api_key_here

# Supabase Configuration
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_ANON_KEY=your_anon_key_here

# Redis Configuration (Railway provides Redis service)
# If using Railway Redis, use the REDIS_URL provided by Railway
REDIS_URL=redis://default:password@host:port
# IMPORTANT: Use the SAME Redis URL as your workers

# Environment
ENVIRONMENT=production

# CORS - Set to your Netlify frontend URL
ALLOWED_ORIGINS=https://your-app.netlify.app,https://your-app.netlify.app/*
```

**Important:** The `REDIS_URL` **MUST** be the same as your workers. Both services connect to the same Redis instance.

#### Optional Variables (with defaults)

```
# Server Configuration
API_HOST=0.0.0.0
API_PORT=8000
# Note: Railway sets PORT automatically, API_PORT is fallback

# AI Model Configuration
TUTOR_MODEL=gpt-4o-mini
GRADING_MODEL=gpt-4o-mini
HELPING_MODEL=gpt-4o-mini

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json
ENABLE_DEBUG=false

# LangSmith (Optional)
LANGSMITH_TRACING=false
LANGSMITH_API_KEY=
LANGSMITH_PROJECT=imtehaan-ai-tutor
```

**Important:** See `config.env.example` for all available configuration options.

### Step 4: Add Redis Service (Recommended)

1. In Railway Dashboard, click "New" → "Database" → "Redis"
2. Railway will provide `REDIS_URL` automatically
3. Add `REDIS_URL` to your API service environment variables
4. **IMPORTANT:** Use the same `REDIS_URL` in your workers service

### Step 5: Deploy

Railway will automatically:
1. Detect Python from `runtime.txt`
2. Install dependencies from `requirements.txt`
3. Run the command from `Procfile`: `python start_unified_backend.py`

### Step 6: Get Your Backend URL

1. Go to Railway Dashboard → Your Service → Settings
2. Generate a domain or use the provided one
3. Copy the URL (e.g., `https://your-backend.up.railway.app`)

### Step 7: Update Frontend Configuration

In your Netlify deployment, set environment variable:
```
VITE_API_BASE_URL=https://your-backend.up.railway.app
```

Or update `netlify.toml` redirect:
```toml
[[redirects]]
  from = "/api/*"
  to = "https://your-backend.up.railway.app/:splat"
  status = 200
```

## 🔧 Configuration

### CORS Settings

The backend is configured to accept requests from your Netlify frontend. Set:

```
ALLOWED_ORIGINS=https://your-app.netlify.app,https://your-app.netlify.app/*
```

**Security Note:** Never use `*` in production. Always specify exact domains.

### Port Configuration

Railway automatically sets the `PORT` environment variable. The backend will:
1. Use `PORT` if set (Railway provides this)
2. Fall back to `API_PORT` if set
3. Default to `8000` for local development

### Environment Detection

Set `ENVIRONMENT=production` to:
- Enable production optimizations
- Disable debug mode
- Enforce stricter CORS
- Use production logging

## 📊 Health Checks

Railway automatically monitors your service. The backend provides:

- **Health Endpoint:** `GET /health`
- **API Docs:** `GET /docs`
- **OpenAPI Spec:** `GET /openapi.json`
- **Worker Health:** `GET /observability/worker-health`

## 🔍 Monitoring

### Logs

View logs in Railway Dashboard → Your Service → Deployments → View Logs

### Metrics

Railway provides:
- Request metrics
- Error rates
- Response times
- Resource usage

## 🔗 Integration with Workers

### Architecture

```
Frontend (Netlify)
    ↓
API Server (Railway - this service)
    ↓ (enqueues jobs)
Redis Queue
    ↓ (processes jobs)
Workers (Railway - separate service)
    ↓ (stores results)
Redis
    ↓ (API server retrieves results)
API Server → Frontend
```

### Required Setup

1. **API Server** (this repository/service)
   - Handles HTTP requests
   - Enqueues jobs to Redis
   - Retrieves results from Redis

2. **Workers** (separate repository/service)
   - Processes jobs from Redis
   - Executes AI operations
   - Stores results in Redis

3. **Redis** (shared)
   - Both services connect to same Redis
   - Jobs are queued and results stored here

**Important:** Both services MUST use the same `REDIS_URL`.

## 🐛 Troubleshooting

### Build Fails

1. **Check Python version**
   - Ensure `runtime.txt` specifies compatible version (3.11.0)
   - Railway supports Python 3.8+

2. **Check dependencies**
   - Verify `requirements.txt` is present
   - Check for dependency conflicts

3. **Check logs**
   - View build logs in Railway Dashboard
   - Look for import errors or missing dependencies

### Service Won't Start

1. **Check environment variables**
   - Verify all required variables are set
   - Check for typos in variable names

2. **Check CORS configuration**
   - Ensure `ALLOWED_ORIGINS` includes your Netlify domain
   - Verify no trailing slashes or incorrect URLs

3. **Check Redis connection**
   - Verify `REDIS_URL` is correct
   - Test Redis connection from Railway logs

### API Calls Failing

1. **Check CORS**
   - Verify frontend domain is in `ALLOWED_ORIGINS`
   - Check browser console for CORS errors

2. **Check backend URL**
   - Verify `VITE_API_BASE_URL` in Netlify matches Railway URL
   - Test backend health endpoint directly

3. **Check authentication**
   - Verify Supabase keys are correct
   - Check for authentication errors in logs

4. **Check workers**
   - Verify workers are running (separate service)
   - Check worker health endpoint: `/observability/worker-health`
   - Ensure workers are processing jobs

## 📝 Environment Variables Reference

See `config.env.example` for complete list of all environment variables with descriptions.

### Critical Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `OPENAI_API_KEY` | ✅ Yes | OpenAI API key for AI models |
| `SUPABASE_URL` | ✅ Yes | Supabase project URL |
| `SUPABASE_ANON_KEY` | ✅ Yes | Supabase anonymous key |
| `REDIS_URL` | ✅ Yes | Redis connection URL (must match workers) |
| `ENVIRONMENT` | ✅ Yes | `production` for Railway |
| `ALLOWED_ORIGINS` | ✅ Yes | Netlify frontend URL(s) |

## 🔗 Integration with Netlify Frontend

### Frontend Configuration

In Netlify, set:
```
VITE_API_BASE_URL=https://your-backend.up.railway.app
```

### API Proxy (Alternative)

Alternatively, use Netlify redirects in `netlify.toml`:
```toml
[[redirects]]
  from = "/api/*"
  to = "https://your-backend.up.railway.app/:splat"
  status = 200
```

Then frontend calls `/api/tutor/chat` which proxies to Railway backend.

## ✅ Deployment Checklist

- [ ] Railway project created
- [ ] Repository connected
- [ ] All environment variables set
- [ ] Redis service added (if needed)
- [ ] CORS configured with Netlify domain
- [ ] Backend deployed successfully
- [ ] Health endpoint responding
- [ ] Workers service deployed (separate repository)
- [ ] Workers connected to same Redis
- [ ] Frontend configured with backend URL
- [ ] Test API calls from frontend

## 🎯 Next Steps

After deployment:

1. **Test Backend**
   - Visit `https://your-backend.up.railway.app/health`
   - Visit `https://your-backend.up.railway.app/docs`

2. **Deploy Workers** (separate repository)
   - Deploy workers service
   - Use same `REDIS_URL`
   - Verify workers are processing jobs

3. **Test Frontend Connection**
   - Open Netlify frontend
   - Test login/signup
   - Test AI Tutor chat
   - Check browser console for errors

4. **Monitor Performance**
   - Check Railway metrics
   - Monitor error rates
   - Review logs for issues

5. **Set Up Custom Domain** (Optional)
   - Configure custom domain in Railway
   - Update CORS settings
   - Update frontend configuration

---

**Ready to deploy!** This repository is self-contained and can be deployed independently from the workers.
#   i m t e h a a n a i  
 
# Railway Environment Variables Setup Guide

## 🚀 Quick Start - Minimum Required Variables

Copy and paste these into Railway's Variables tab:

```env
OPENAI_API_KEY=your_openai_api_key_here
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_ANON_KEY=your_anon_key_here
REDIS_URL=redis://default:password@host:port
ENVIRONMENT=production
ALLOWED_ORIGINS=https://imtehaanai.netlify.app,https://imtehaanai.netlify.app/*
ALLOW_CREDENTIALS=true
```

---

## 📝 Step-by-Step Setup

### Step 1: Get Your API Keys

#### OpenAI API Key
1. Go to https://platform.openai.com/api-keys
2. Click "Create new secret key"
3. Copy the key (starts with `sk-proj-` or `sk-`)

#### Supabase Credentials
1. Go to https://supabase.com/dashboard
2. Select your project
3. Go to **Settings** → **API**
4. Copy:
   - **Project URL** → `SUPABASE_URL`
   - **anon public** key → `SUPABASE_ANON_KEY`

#### Redis URL (Railway)
1. In Railway, add a **Redis** service to your project
2. Railway will automatically provide `REDIS_URL`
3. Copy the `REDIS_URL` from the Redis service variables

---

### Step 2: Set Variables in Railway

1. Go to [Railway Dashboard](https://railway.app)
2. Select your **API server** service
3. Click on **"Variables"** tab
4. Click **"New Variable"** for each variable

---

### Step 3: Required Variables (Set These First)

```env
# OpenAI
OPENAI_API_KEY=sk-proj-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# Supabase
SUPABASE_URL=https://xxxxxxxxxxxxx.supabase.co
SUPABASE_ANON_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.xxxxxxxxxxxxx

# Redis (from Railway Redis service)
REDIS_URL=redis://default:xxxxxxxxxxxxx@containers-us-west-xxx.railway.app:xxxxx

# Environment
ENVIRONMENT=production

# CORS (Your Netlify frontend)
ALLOWED_ORIGINS=https://imtehaanai.netlify.app,https://imtehaanai.netlify.app/*
ALLOW_CREDENTIALS=true
```

---

### Step 4: Optional Variables (Customize as Needed)

These have sensible defaults, but you can customize them:

#### AI Model Configuration
```env
TUTOR_MODEL=gpt-4o-mini
GRADING_MODEL=gpt-4o-mini
HELPING_MODEL=gpt-4o-mini
```

#### Logging
```env
LOG_LEVEL=INFO
LOG_FORMAT=json
ENABLE_DEBUG=false
```

#### Rate Limiting (Adjust based on your needs)
```env
RATE_LIMIT_TUTOR_CHAT_REQUESTS=60
RATE_LIMIT_ANSWER_GRADING_REQUESTS=100
RATE_LIMIT_MOCK_EXAM_GRADING_REQUESTS=10
RATE_LIMIT_CONCEPT_EXPLANATION_REQUESTS=200
RATE_LIMIT_LESSON_CREATION_REQUESTS=30
```

#### Worker Configuration
```env
WORKER_CONCURRENCY=3
MAX_DB_CONNECTIONS=5
```

---

## 🔍 Verification

After setting variables, check:

1. **Deploy Logs**: Check Railway deployment logs for any errors
2. **Health Check**: Visit `https://your-backend.up.railway.app/health`
3. **API Docs**: Visit `https://your-backend.up.railway.app/docs`

---

## ⚠️ Important Notes

### Redis URL
- **MUST** use the same `REDIS_URL` in both API server and workers
- Railway provides this automatically when you add Redis service
- Format: `redis://default:password@host:port`

### CORS Configuration
- **MUST** include your Netlify frontend URL
- Format: `https://imtehaanai.netlify.app,https://imtehaanai.netlify.app/*`
- Never use `*` in production (security risk)

### Environment
- Set `ENVIRONMENT=production` for production deployment
- This enables production optimizations and safety checks

### Port
- Railway automatically provides `PORT` environment variable
- Your code uses it automatically - **don't set it manually**

---

## 📋 Complete Variable List

For the complete list of all available environment variables, see:
- **`COMPLETE_ENV_VARIABLES.md`** - Full documentation
- **`config.env.example`** - Template with all variables and descriptions

---

## 🆘 Troubleshooting

### "OPENAI_API_KEY not found"
- Make sure you set `OPENAI_API_KEY` in Railway variables
- Check for typos in the variable name

### "Redis connection failed"
- Verify `REDIS_URL` is set correctly
- Make sure Redis service is running in Railway
- Check that API server and workers use the same `REDIS_URL`

### "CORS error" in frontend
- Verify `ALLOWED_ORIGINS` includes your Netlify URL
- Format: `https://imtehaanai.netlify.app,https://imtehaanai.netlify.app/*`
- Make sure `ALLOW_CREDENTIALS=true`

### "Supabase connection failed"
- Verify `SUPABASE_URL` and `SUPABASE_ANON_KEY` are correct
- Check Supabase project is active
- Verify network access (Supabase allows all IPs by default)

---

## ✅ Success Checklist

- [ ] All required variables set
- [ ] Redis service added and `REDIS_URL` copied
- [ ] CORS configured with Netlify URL
- [ ] `ENVIRONMENT=production` set
- [ ] Deployment successful (check logs)
- [ ] Health check endpoint responds
- [ ] API documentation accessible

---

**Ready to deploy!** 🚀

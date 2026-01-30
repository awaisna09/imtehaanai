# Netlify Frontend URL Configuration

## 🌐 Frontend URL

**Your Netlify Frontend:** https://imtehaanai.netlify.app/

---

## ⚙️ Required Backend Configuration

### Set ALLOWED_ORIGINS Environment Variable

In **Railway Dashboard** → **Your API Service** → **Variables**, set:

```env
ALLOWED_ORIGINS=https://imtehaanai.netlify.app,https://imtehaanai.netlify.app/*
```

**Why:**
- Allows CORS requests from your Netlify frontend
- Required for frontend to communicate with backend
- No localhost needed (production only)

---

## 📋 Complete Railway Backend Environment Variables

```env
# Core Infrastructure
OPENAI_API_KEY=your_key
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_ANON_KEY=your_key
REDIS_URL=redis://... (Railway provides this)

# Environment
ENVIRONMENT=production

# CORS - Set to your Netlify frontend URL
ALLOWED_ORIGINS=https://imtehaanai.netlify.app,https://imtehaanai.netlify.app/*

# Other variables (see config.env.example)
```

---

## ✅ After Setting ALLOWED_ORIGINS

1. **Restart Railway Service**
   - Changes take effect immediately
   - Or wait for next deployment

2. **Test Connection**
   - Visit https://imtehaanai.netlify.app/
   - Try to login or make an API call
   - Check browser console for CORS errors

3. **Verify CORS**
   - If CORS errors appear, double-check `ALLOWED_ORIGINS` value
   - Ensure URL matches exactly (no typos)

---

**Frontend URL:** https://imtehaanai.netlify.app/

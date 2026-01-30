# ENVIRONMENT Variable Setting for Railway

## ⚠️ IMPORTANT: Railway Deployment Should Use `ENVIRONMENT=production`

### Current Issue
If your Railway deployment has `ENVIRONMENT=development`, you may experience:
1. **Auto-reload enabled** - Server tries to watch for file changes (not needed on Railway)
2. **DEBUG log level** - More verbose logging (uses more resources)
3. **Supabase safety check** - May block production Supabase URLs
4. **Localhost CORS origins** - Adds unnecessary localhost origins

---

## ✅ Recommended: Set `ENVIRONMENT=production` in Railway

### Why Production Mode?
- **No auto-reload** - Railway handles deployments, no need for file watching
- **INFO log level** - Balanced logging without excessive verbosity
- **No Supabase blocking** - Allows production Supabase URLs
- **Cleaner CORS** - Only allows specified origins (no localhost)
- **Stricter validation** - Warnings treated as errors (catches issues early)

### How to Set in Railway:
1. Go to Railway Dashboard → Your Project → Variables
2. Find `ENVIRONMENT` variable
3. Set value to: `production`
4. Save and redeploy

---

## 🔧 Alternative: Keep Development Mode

If you want to keep `ENVIRONMENT=development` for Railway, you MUST also set:

```env
ALLOW_PROD_SUPABASE_IN_DEV=true
```

**Why?** The code blocks production Supabase URLs in non-production environments to prevent accidental data corruption. Setting this flag bypasses that check.

### Development Mode Behavior:
- ✅ Auto-reload enabled (watches for file changes)
- ✅ DEBUG log level (more verbose)
- ✅ Localhost CORS origins added
- ⚠️ Requires `ALLOW_PROD_SUPABASE_IN_DEV=true` if using production Supabase
- ⚠️ Config validation warnings don't fail deployment
- ✅ **Redis Configuration Priority**: If `REDIS_HOST`, `REDIS_PORT`, or `REDIS_PASSWORD` are set, they take priority over `REDIS_URL` (allows bypassing broken REDIS_URL)

---

## 📋 Environment Variable Comparison

| Setting | Development | Production |
|---------|------------|------------|
| Auto-reload | ✅ Enabled | ❌ Disabled |
| Log Level | DEBUG | INFO |
| CORS Origins | Includes localhost | Only specified origins |
| Supabase Check | Blocks prod URLs* | Allows prod URLs |
| Config Validation | Warnings allowed | Warnings = errors |

*Unless `ALLOW_PROD_SUPABASE_IN_DEV=true` is set

---

## 🚀 Quick Fix

**For Railway deployment, set:**
```env
ENVIRONMENT=production
```

This is the recommended setting for all Railway deployments, even if you're still testing/developing. Railway handles code deployments, so auto-reload is not needed.

---

## 📝 Current Railway Configuration

If you're currently using `ENVIRONMENT=development` in Railway:

1. **Check if Supabase is working** - If you get Supabase errors, you need `ALLOW_PROD_SUPABASE_IN_DEV=true`
2. **Consider switching to production** - It's better for deployed environments
3. **Update CORS** - Make sure `ALLOWED_ORIGINS` includes your Netlify URL
4. **Redis Configuration** - If `REDIS_URL` doesn't work, set `REDIS_HOST`, `REDIS_PORT`, and `REDIS_PASSWORD` instead (they take priority in development mode)

---

## 🔧 Redis Configuration in Development Mode

### Priority Order (Development Mode):
1. **If `REDIS_HOST`, `REDIS_PORT`, or `REDIS_PASSWORD` are set** → Use separate variables (bypasses `REDIS_URL`)
2. **If only `REDIS_URL` is set** → Use `REDIS_URL`
3. **If neither is set** → Fall back to defaults (localhost:6379)

### Example: Using Separate Variables (When REDIS_URL doesn't work)

```env
ENVIRONMENT=development
REDIS_HOST=switchyard.proxy.rlwy.net
REDIS_PORT=19437
REDIS_PASSWORD=BpymepesTceqpFGWXfQxwbJpaqXbqldH
REDIS_DB=0
# Don't set REDIS_URL, or remove it if it's broken
```

**Note:** In development mode, setting `REDIS_HOST`, `REDIS_PORT`, or `REDIS_PASSWORD` will automatically bypass `REDIS_URL`, even if `REDIS_URL` is also set. This allows you to use separate variables when `REDIS_URL` doesn't work.

---

## 🔍 How to Check Current Setting

In Railway logs, you'll see:
- `✅ Production mode: Using Railway environment variables` (if production)
- `✅ Loaded config.env file (development mode)` (if development)

Or check Railway Dashboard → Variables → `ENVIRONMENT`

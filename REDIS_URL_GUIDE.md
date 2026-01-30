# How to Get Correct Redis URL from Railway

## ⚠️ Important: Redis is NOT an HTTP Service

**The 502 Bad Gateway error is NORMAL!**

Redis is a **TCP service**, not an HTTP service. You **cannot** access it via a web browser. The 502 error is expected because Redis doesn't serve HTTP requests.

---

## ✅ How to Get the Correct REDIS_URL

### Step 1: Go to Railway Dashboard
1. Open [Railway Dashboard](https://railway.app)
2. Select your project
3. Find your **Redis service** (named something like "Redis" or "redis-production-9890")

### Step 2: Get REDIS_URL from Variables
1. Click on your **Redis service**
2. Click on **"Variables"** tab (or **"Settings"** → **"Variables"**)
3. Look for a variable named **`REDIS_URL`**
4. **Copy the entire value** - it should look like:
   ```
   redis://default:password@redis-production-9890.up.railway.app:port
   ```
   OR
   ```
   redis://default:password@containers-us-west-xxx.railway.app:xxxxx
   ```

### Step 3: Use the REDIS_URL Value
The `REDIS_URL` variable contains the **correct connection string** with:
- Protocol: `redis://`
- Username: `default` (Railway's default)
- Password: (automatically generated)
- Host: (your Redis service hostname)
- Port: (Redis port, usually 6379 or a Railway-assigned port)

---

## 📋 Example REDIS_URL Format

```
redis://default:abc123xyz@redis-production-9890.up.railway.app:6379
```

**Format breakdown:**
- `redis://` - Protocol
- `default` - Username (Railway default)
- `abc123xyz` - Password (Railway generates this)
- `redis-production-9890.up.railway.app` - Hostname
- `6379` - Port

---

## 🔧 If REDIS_URL Variable Doesn't Exist

### Option 1: Railway Auto-Generates It
Railway should automatically create `REDIS_URL` when you add a Redis service. If it's missing:

1. Check **"Variables"** tab in Redis service
2. Look for any Redis-related variables
3. Railway might name it differently in some cases

### Option 2: Construct It Manually
If you have the components, you can construct it:

1. **Hostname**: `redis-production-9890.up.railway.app` (from your Redis service)
2. **Port**: Check Redis service → **"Settings"** → **"Port"** (usually 6379)
3. **Password**: Check Redis service → **"Variables"** → Look for `REDIS_PASSWORD` or similar
4. **Username**: Usually `default` for Railway

**Format:**
```
redis://default:YOUR_PASSWORD@redis-production-9890.up.railway.app:YOUR_PORT
```

---

## ✅ Steps to Fix Your Connection

### 1. Get REDIS_URL from Railway
- Go to Redis service → Variables tab
- Copy `REDIS_URL` value

### 2. Update API Service Variables
- Go to your **API service** in Railway
- Click **"Variables"** tab
- Find `REDIS_URL` variable
- **Paste the value** from Redis service
- Save

### 3. Redeploy
- Railway will automatically redeploy with new variables
- Check logs to verify Redis connection

---

## 🔍 Verify Redis Connection

### Check Railway Logs
1. Go to API service → **"Deployments"** tab
2. Click on latest deployment
3. Click **"View Logs"**
4. Look for: `✅ Redis connected` or `❌ Redis connection failed`

### Expected Log Messages
**Success:**
```
✅ Redis connected: production environment | URL-based:N/A
```

**Failure:**
```
❌ Redis connection failed: Error 22 connecting to...
```

---

## 🆘 Still Having Issues?

### Check These:
1. ✅ Redis service is **running** (not stopped)
2. ✅ Redis service is in the **same Railway project** as your API
3. ✅ `REDIS_URL` is copied **exactly** (no extra spaces, quotes, or newlines)
4. ✅ `ENVIRONMENT=production` is set in API service
5. ✅ API service has been **redeployed** after updating variables

### Alternative: Use Separate Variables
If `REDIS_URL` still doesn't work, use separate variables:

```env
REDIS_HOST=redis-production-9890.up.railway.app
REDIS_PORT=6379
REDIS_PASSWORD=your_password_here
REDIS_DB=0
ENVIRONMENT=development
```

**Note:** Set `ENVIRONMENT=development` when using separate variables (production requires `REDIS_URL`).

---

## 📝 Quick Checklist

- [ ] Redis service is running in Railway
- [ ] Found `REDIS_URL` in Redis service → Variables tab
- [ ] Copied `REDIS_URL` value (entire string)
- [ ] Pasted into API service → Variables → `REDIS_URL`
- [ ] No extra spaces or quotes around value
- [ ] API service redeployed
- [ ] Checked logs for connection status

---

## 💡 Key Points

1. **502 Bad Gateway is NORMAL** - Redis doesn't serve HTTP
2. **Use REDIS_URL from Variables** - Not the public URL
3. **Format is important** - Must be `redis://default:password@host:port`
4. **Same project** - Redis and API must be in same Railway project
5. **Redeploy required** - Changes take effect after redeploy

---

**The Redis URL you need is in Railway's Variables, not accessible via browser!** 🔑

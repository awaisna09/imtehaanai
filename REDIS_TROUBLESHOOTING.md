# Redis Connection Troubleshooting Guide

## Error: "Error 22 connecting to switchyard.proxy.rlwy.net:19437. Invalid argument"

This error typically means:
1. **Redis URL is incorrect or outdated**
2. **Redis service is not running in Railway**
3. **Network connectivity issue**
4. **URL format issue**

---

## ✅ Solution 1: Get Fresh Redis URL from Railway

### Step 1: Check Railway Redis Service
1. Go to [Railway Dashboard](https://railway.app)
2. Open your project
3. Find your **Redis service** (or add one if missing)
4. Click on the Redis service

### Step 2: Get the Correct REDIS_URL
1. In Redis service, go to **"Variables"** tab
2. Look for `REDIS_URL` variable
3. **Copy the exact value** (Railway generates this automatically)
4. It should look like: `redis://default:password@host:port`

### Step 3: Update Environment Variable
1. Go to your **API server** service
2. Click **"Variables"** tab
3. Find `REDIS_URL` variable
4. **Replace** with the value from Redis service
5. Save and redeploy

---

## ✅ Solution 2: Use Separate Redis Variables (If URL Doesn't Work)

If `REDIS_URL` format causes issues, use separate variables:

### In Railway Variables, set:
```env
REDIS_HOST=switchyard.proxy.rlwy.net
REDIS_PORT=19437
REDIS_PASSWORD=BpymepesTceqpFGWXfQxwbJpaqXbqldH
REDIS_DB=0
```

**Important:** Leave `REDIS_URL` **empty** or **remove it** when using separate variables.

---

## ✅ Solution 3: Verify Redis Service is Running

1. In Railway Dashboard → Redis service
2. Check **"Deployments"** tab
3. Ensure service shows **"Active"** or **"Running"**
4. If not running, click **"Deploy"** or **"Restart"**

---

## ✅ Solution 4: Check Redis URL Format

The correct format should be:
```
redis://default:password@host:port
```

**Common Issues:**
- ❌ Missing `redis://` prefix
- ❌ Wrong username (should be `default` for Railway)
- ❌ Special characters in password not URL-encoded
- ❌ Extra spaces or newlines

**Correct Example:**
```
redis://default:BpymepesTceqpFGWXfQxwbJpaqXbqldH@switchyard.proxy.rlwy.net:19437
```

---

## ✅ Solution 5: Test Redis Connection

### Option A: Test from Railway Terminal
1. In Railway Dashboard → Your API service
2. Click **"Deployments"** → Latest deployment → **"View Logs"**
3. Or use **"Terminal"** tab
4. Run:
```bash
python -c "import redis; r = redis.from_url('redis://default:BpymepesTceqpFGWXfQxwbJpaqXbqldH@switchyard.proxy.rlwy.net:19437'); print(r.ping())"
```

### Option B: Test from Local Machine
```bash
redis-cli -h switchyard.proxy.rlwy.net -p 19437 -a BpymepesTceqpFGWXfQxwbJpaqXbqldH ping
```

---

## ✅ Solution 6: Add Redis Service (If Missing)

If you don't have a Redis service:

1. In Railway Dashboard → Your project
2. Click **"+ New"** → **"Database"** → **"Add Redis"**
3. Railway will create Redis service
4. Go to Redis service → **"Variables"** tab
5. Copy `REDIS_URL` value
6. Add to your API server variables

---

## 🔍 Debugging Steps

### 1. Check Environment Variables
In Railway → API service → Variables:
- [ ] `REDIS_URL` is set (or `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`)
- [ ] `ENVIRONMENT=production` (or `development` if testing)
- [ ] No extra spaces or quotes around values

### 2. Check Logs
In Railway → API service → Deployments → View Logs:
- Look for Redis connection errors
- Check if Redis URL is being read correctly
- Verify environment is set correctly

### 3. Verify Redis Service
- Redis service is running
- Redis service is in the same Railway project
- Redis service has `REDIS_URL` variable set

---

## 📋 Quick Fix Checklist

- [ ] Redis service exists and is running in Railway
- [ ] Copied `REDIS_URL` from Redis service (not from old config)
- [ ] `REDIS_URL` has correct format: `redis://default:password@host:port`
- [ ] No extra spaces or quotes in `REDIS_URL`
- [ ] `ENVIRONMENT=production` is set
- [ ] API service redeployed after updating variables
- [ ] Checked Railway logs for detailed error messages

---

## 🆘 Still Not Working?

### Try Alternative: Use Separate Variables

Remove `REDIS_URL` and use:
```env
REDIS_HOST=switchyard.proxy.rlwy.net
REDIS_PORT=19437
REDIS_PASSWORD=BpymepesTceqpFGWXfQxwbJpaqXbqldH
REDIS_DB=0
ENVIRONMENT=development
```

**Note:** Set `ENVIRONMENT=development` temporarily to allow separate variables (production mode requires `REDIS_URL`).

---

## 📞 Need More Help?

1. Check Railway documentation: https://docs.railway.app/databases/redis
2. Verify Redis service is accessible from your API service
3. Check Railway status page for outages
4. Review Railway logs for detailed error messages

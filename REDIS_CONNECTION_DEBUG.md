# Redis Connection Debugging Guide

## ✅ Redis URL is Already Set - But Still Getting Error 22

If `REDIS_URL` is already in Railway variables but you're still getting "Error 22", check these:

---

## 🔍 Common Issues When URL is Set

### 1. Extra Spaces or Quotes
**Problem:** Railway might add spaces or quotes around the value

**Check:**
- In Railway → Variables → `REDIS_URL`
- Make sure there are **NO spaces** before or after the value
- Make sure there are **NO quotes** around the value

**Correct:**
```
redis://default:BpymepesTceqpFGWXfQxwbJpaqXbqldH@switchyard.proxy.rlwy.net:19437
```

**Wrong:**
```
"redis://default:BpymepesTceqpFGWXfQxwbJpaqXbqldH@switchyard.proxy.rlwy.net:19437"
```
or
```
 redis://default:BpymepesTceqpFGWXfQxwbJpaqXbqldH@switchyard.proxy.rlwy.net:19437 
```

---

### 2. Environment Variable Not Set
**Problem:** `ENVIRONMENT=production` might not be set

**Check:**
- Railway → API service → Variables
- Verify `ENVIRONMENT=production` is set
- If missing, add it

**Why it matters:**
- Production mode uses `REDIS_URL` directly
- Development mode might look for separate variables first

---

### 3. Conflicting Redis Variables
**Problem:** Both `REDIS_URL` and separate variables (`REDIS_HOST`, etc.) might be set

**Check:**
- Railway → API service → Variables
- If you have `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD` set, **remove them**
- Keep **only** `REDIS_URL` when `ENVIRONMENT=production`

---

### 4. Redis Service Not Accessible
**Problem:** Network connectivity issue between services

**Check:**
1. Redis service is **running** (not stopped)
2. Redis service is in the **same Railway project**
3. Both services are in the **same region** (if applicable)

---

### 5. URL Encoding Issues
**Problem:** Special characters in password might need encoding

**Check:**
- Your password: `BpymepesTceqpFGWXfQxwbJpaqXbqldH`
- This looks fine (no special characters that need encoding)
- If Railway shows it differently, use the exact value from Redis service

---

## 🔧 Step-by-Step Fix

### Step 1: Verify Redis URL Format
1. Go to Railway → Redis service → Variables
2. Copy `REDIS_URL` value exactly
3. Go to Railway → API service → Variables
4. Delete existing `REDIS_URL`
5. Add new `REDIS_URL` and paste the value
6. **Check for spaces/quotes** - remove them if present
7. Save

### Step 2: Check Environment
1. Railway → API service → Variables
2. Verify `ENVIRONMENT=production` exists
3. If not, add it

### Step 3: Remove Conflicting Variables
1. Railway → API service → Variables
2. Delete these if they exist:
   - `REDIS_HOST`
   - `REDIS_PORT`
   - `REDIS_PASSWORD`
   - `REDIS_DB`
3. Keep **only** `REDIS_URL`

### Step 4: Redeploy
1. Railway will auto-redeploy
2. Check logs for Redis connection status

---

## 🧪 Test Connection

### Check Railway Logs
1. Railway → API service → Deployments
2. Click latest deployment → View Logs
3. Look for:
   - ✅ `Redis connected: production environment`
   - ❌ `Redis connection failed: Error 22`

### Expected Log Output
**Success:**
```
✅ Redis connected: production environment | URL-based:N/A
```

**Failure:**
```
❌ Redis connection failed: Error 22 connecting to switchyard.proxy.rlwy.net:19437. Invalid argument.
```

---

## 🔄 Alternative: Use Separate Variables

If `REDIS_URL` still doesn't work, try separate variables:

### Step 1: Remove REDIS_URL
- Railway → API service → Variables
- Delete `REDIS_URL`

### Step 2: Add Separate Variables
```env
REDIS_HOST=switchyard.proxy.rlwy.net
REDIS_PORT=19437
REDIS_PASSWORD=BpymepesTceqpFGWXfQxwbJpaqXbqldH
REDIS_DB=0
```

### Step 3: Change Environment
```env
ENVIRONMENT=development
```

**Note:** Development mode supports separate variables, production requires `REDIS_URL`

---

## 🐛 Debug: Check What Code Sees

The error "Error 22" is a system-level error that usually means:
- Invalid host/port combination
- Network connectivity issue
- Socket configuration problem

### Check These in Railway Logs:
1. What URL is being used (check startup logs)
2. Any connection timeout errors
3. Network-related errors

---

## ✅ Quick Checklist

- [ ] `REDIS_URL` has no spaces or quotes
- [ ] `ENVIRONMENT=production` is set
- [ ] No conflicting Redis variables (`REDIS_HOST`, etc.)
- [ ] Redis service is running
- [ ] Both services in same Railway project
- [ ] Service has been redeployed after changes
- [ ] Checked logs for detailed error messages

---

## 🆘 Still Not Working?

### Try This:
1. **Temporarily set `ENVIRONMENT=development`**
2. **Use separate variables** (see above)
3. **Check if connection works in development mode**
4. **If it works, the issue is with URL parsing in production mode**

### Contact Railway Support:
If nothing works, the issue might be:
- Railway network configuration
- Redis service access restrictions
- Regional connectivity issues

---

## 📋 Summary

Since `REDIS_URL` is already set, the most common issues are:
1. **Extra spaces/quotes** around the value
2. **Missing `ENVIRONMENT=production`**
3. **Conflicting variables** (`REDIS_HOST`, etc.)
4. **Network connectivity** between services

Check these first, then try the separate variables approach if needed.

# Redis Configuration in Development Mode

## 🔧 Issue: REDIS_URL Doesn't Work in Development

If you're using `ENVIRONMENT=development` in Railway and `REDIS_URL` doesn't work, you can use separate Redis configuration variables instead.

---

## ✅ Solution: Use Separate Redis Variables

In **development mode**, if you set `REDIS_HOST`, `REDIS_PORT`, or `REDIS_PASSWORD`, they will **automatically take priority** over `REDIS_URL`, even if `REDIS_URL` is also set.

### Configuration Priority (Development Mode):
1. **Separate variables** (`REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`) - **HIGHEST PRIORITY**
2. `REDIS_URL` - Only used if separate variables are not set
3. Defaults (localhost:6379) - Fallback if nothing is set

---

## 📋 How to Configure

### Option 1: Remove REDIS_URL and Use Separate Variables

In Railway Variables, set:

```env
ENVIRONMENT=development
REDIS_HOST=switchyard.proxy.rlwy.net
REDIS_PORT=19437
REDIS_PASSWORD=BpymepesTceqpFGWXfQxwbJpaqXbqldH
REDIS_DB=0
```

**Important:** Don't set `REDIS_URL` at all, or remove it if it exists.

### Option 2: Keep REDIS_URL but Set Separate Variables

If you set `REDIS_HOST`, `REDIS_PORT`, or `REDIS_PASSWORD`, they will automatically override `REDIS_URL`:

```env
ENVIRONMENT=development
REDIS_URL=redis://broken-url-here  # This will be ignored
REDIS_HOST=switchyard.proxy.rlwy.net
REDIS_PORT=19437
REDIS_PASSWORD=BpymepesTceqpFGWXfQxwbJpaqXbqldH
REDIS_DB=0
```

---

## 🔍 How It Works

The code checks for separate Redis variables first in development mode:

```python
# Development mode logic
if redis_host or redis_port or redis_password:
    # Use separate variables (bypasses REDIS_URL)
    self.host = redis_host or "localhost"
    self.port = int(redis_port or 6379)
    self.password = redis_password or None
    self.url = None
else:
    # Fall back to REDIS_URL if no separate variables
    redis_url = os.getenv("REDIS_URL")
    if redis_url:
        self.url = redis_url
```

---

## ⚠️ Production Mode Behavior

**Note:** In `ENVIRONMENT=production`, `REDIS_URL` is **REQUIRED** and separate variables are **NOT** used. Production mode only supports `REDIS_URL`.

If you need to use separate variables, you must use `ENVIRONMENT=development` (or `staging`).

---

## 🚀 Quick Setup

1. Go to Railway Dashboard → Your Project → Variables
2. Set `ENVIRONMENT=development` (if not already set)
3. Set these Redis variables:
   - `REDIS_HOST=switchyard.proxy.rlwy.net`
   - `REDIS_PORT=19437`
   - `REDIS_PASSWORD=BpymepesTceqpFGWXfQxwbJpaqXbqldH`
   - `REDIS_DB=0`
4. **Remove or leave `REDIS_URL` unset** (or it will be ignored if separate variables are set)
5. Save and redeploy

---

## ✅ Verification

After deployment, check Railway logs. You should see:

```
✅ Redis connected: development environment | switchyard.proxy.rlwy.net:19437
```

If you see "URL-based" instead of the host:port, it means `REDIS_URL` is being used instead of separate variables.

# How to Get Your Railway Backend URL

## 🚀 Quick Steps

### Step 1: Go to Railway Dashboard
1. Open [Railway Dashboard](https://railway.app)
2. Sign in to your account
3. Select your project

### Step 2: Find Your API Service
1. In your project, find your **API server** service (the one you deployed)
2. Click on the service name

### Step 3: Get the URL
1. Click on the **"Settings"** tab
2. Scroll down to **"Domains"** section
3. You'll see your Railway URL, for example:
   ```
   https://your-service-name.up.railway.app
   ```

**OR**

1. Click on the **"Deployments"** tab
2. Click on the latest deployment
3. The URL is shown at the top or in the deployment details

---

## 🌐 Alternative: Generate a Custom Domain

### Option 1: Use Railway's Auto-Generated Domain
- Railway automatically provides: `https://your-service-name.up.railway.app`
- This is available immediately after deployment

### Option 2: Add a Custom Domain
1. Go to your service → **Settings** → **Domains**
2. Click **"Generate Domain"** or **"Add Domain"**
3. Railway will provide a domain like: `https://your-service-name.up.railway.app`
4. Or add your own custom domain

---

## 📋 What to Do with the URL

### 1. Update Netlify Frontend
Once you have your Railway backend URL, update your Netlify frontend:

1. Go to [Netlify Dashboard](https://app.netlify.com)
2. Select your site
3. Go to **Site settings** → **Environment variables**
4. Add or update:
   ```
   VITE_API_BASE_URL=https://your-service-name.up.railway.app
   ```
5. Redeploy your Netlify site

### 2. Test the Backend
Test your backend URL:
- Health check: `https://your-service-name.up.railway.app/health`
- API docs: `https://your-service-name.up.railway.app/docs`
- Root: `https://your-service-name.up.railway.app/`

---

## 🔍 Verify Your Deployment

### Check Deployment Status
1. Railway Dashboard → Your service → **Deployments**
2. Latest deployment should show **"Active"** or **"Success"**

### Check Logs
1. Railway Dashboard → Your service → **Deployments**
2. Click on latest deployment → **"View Logs"**
3. Look for:
   - `✅ Unified backend imported successfully`
   - `🌐 Starting server on http://0.0.0.0:PORT`
   - No errors

---

## ⚠️ Important Notes

1. **URL Changes**: Railway URLs are stable but can change if you delete and recreate the service
2. **HTTPS**: Railway provides HTTPS automatically (no configuration needed)
3. **Port**: Railway sets the `PORT` environment variable automatically - you don't need to specify it
4. **CORS**: Make sure `ALLOWED_ORIGINS` in Railway includes your Netlify URL

---

## 📝 Example Railway URL Format

```
https://imtehaan-backend-api.up.railway.app
```

Or with a custom domain:
```
https://api.imtehaanai.com
```

---

## 🆘 If You Don't See a URL

1. **Check Deployment Status**: Make sure deployment completed successfully
2. **Check Service Settings**: Go to Settings → Domains
3. **Generate Domain**: Click "Generate Domain" if no domain is shown
4. **Check Logs**: Verify the service started correctly

---

## ✅ Quick Checklist

- [ ] Railway service is deployed and active
- [ ] Found URL in Settings → Domains
- [ ] Tested health endpoint: `/health`
- [ ] Updated Netlify `VITE_API_BASE_URL`
- [ ] Verified CORS settings include Netlify URL

---

**Your Railway URL is ready to use!** 🎉

# Database Query Errors Fix

## Issues Identified

### 1. Mock Exam Grading Error
**Error:** `name 'environment' is not defined`

**Location:** `railway-deploy/agents/mock_exam_grading_agent.py` line 3698

**Root Cause:** The variable `environment` is used but never defined before the CORS configuration.

**Fix Applied:**
```python
# Added before the CORS check
environment = os.getenv("ENVIRONMENT", "development").lower()
if environment != "production":
    # Add localhost origins
```

### 2. Concepts Endpoint 500 Errors
**Error:** `[Concepts] Failed to fetch concepts: 500`

**Location:** `railway-deploy/unified_backend.py` endpoint `/concepts/topic/{topic_id}`

**Potential Causes:**
1. Supabase client not initialized
2. Database connection issues
3. Missing or incorrect table names
4. RLS (Row Level Security) policies blocking queries
5. Missing `subject_id` parameter causing wrong table selection

**Current Error Handling:**
The endpoint already has error handling, but we should verify:
- Supabase client is available
- Error messages are clear
- Database queries are properly wrapped

## Fixes Applied

### Fix 1: Mock Exam Grading Agent
- Added `environment = os.getenv("ENVIRONMENT", "development").lower()` before CORS check
- This ensures the variable is defined before use

### Fix 2: Concepts Endpoint (Already Has Error Handling)
The endpoint already has:
- Supabase client check
- Try-catch error handling
- Detailed error logging

However, common issues that cause 500 errors:
1. **Missing `subject_id`**: If `subject_id` is not provided, the concept service might query the wrong table
2. **Table doesn't exist**: Subject-specific concept tables might not exist
3. **RLS policies**: Row Level Security might be blocking queries
4. **Supabase connection**: Client might not be properly initialized

## Testing

### Test Mock Exam Grading
```bash
# Should no longer get "name 'environment' is not defined"
curl -X POST "https://imtehaanai-production.up.railway.app/grade-mock-exam" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "test-user",
    "attempted_questions": [...],
    "subject": "Business Studies",
    "exam_type": "P1"
  }'
```

### Test Concepts Endpoint
```bash
# Test with subject_id
curl "https://imtehaanai-production.up.railway.app/concepts/topic/101?subject_id=101&limit=10"

# Test without subject_id (should still work but might query wrong table)
curl "https://imtehaanai-production.up.railway.app/concepts/topic/101?limit=10"
```

## Common Database Error Causes

### 1. Supabase Client Not Initialized
**Symptom:** 500 errors on all database queries
**Solution:** Check Railway environment variables:
- `SUPABASE_URL`
- `SUPABASE_ANON_KEY` or `SUPABASE_SERVICE_ROLE_KEY`

### 2. RLS Policies Blocking Queries
**Symptom:** 500 errors on specific tables
**Solution:** Check Supabase Dashboard → Authentication → Policies
- Ensure policies allow reads/writes for authenticated users
- Check if service role key is being used (bypasses RLS)

### 3. Missing Tables
**Symptom:** 500 errors with "relation does not exist"
**Solution:** Verify tables exist in Supabase:
- `concepts_business_studies`
- `concepts_economics`
- `concepts_*` (subject-specific tables)
- `time_tracking`
- `study_plans_v2`
- `study_plan_topics_v2`

### 4. Wrong Table Selection
**Symptom:** Concepts endpoint returns empty or wrong data
**Solution:** Ensure `subject_id` is provided in query parameters

## Next Steps

1. **Deploy fixes** to Railway
2. **Check Railway logs** for detailed error messages
3. **Verify Supabase tables** exist and have correct RLS policies
4. **Test endpoints** after deployment
5. **Monitor** for any remaining 500 errors

## Additional Debugging

If errors persist, check:
1. Railway deployment logs for full stack traces
2. Supabase Dashboard → Logs for database query errors
3. Network tab in browser for actual error responses
4. Railway environment variables are correctly set

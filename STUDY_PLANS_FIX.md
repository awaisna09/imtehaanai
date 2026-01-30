# Study Plans Endpoint Fix

## Issue
The `/api/v1/study-plans` endpoint was returning HTTP 500 errors when fetching study plans.

## Root Causes Identified

1. **Missing Query Parameter Validation**: `user_id` was not marked as `Query(...)`, so it could be None or missing
2. **No Supabase Client Check**: The endpoint didn't check if `supabase_client` was available before using it
3. **No Error Handling for Topic Counts**: If fetching topic counts failed, the entire request would fail
4. **No Error Handling for Date Parsing**: If `exam_date` was missing or invalid, the request would fail
5. **No Error Handling for Empty Plan IDs**: If `plan_ids` was empty, the `.in_()` query could fail

## Fixes Applied

### 1. Made `user_id` Required Query Parameter
```python
async def get_study_plans(user_id: str = Query(...)):
```
- Now FastAPI will automatically validate that `user_id` is provided
- Returns 422 if `user_id` is missing

### 2. Added Supabase Client Check
```python
if not supabase_client:
    # Return plans without topic counts
```
- If `supabase_client` is not available, return plans with `topics_count: 0`
- Prevents the endpoint from crashing when Supabase is unavailable

### 3. Added Error Handling for Topic Counts Query
```python
try:
    all_topics_response = sb_execute(...)
except Exception as topics_error:
    # Log error but continue without topic counts
```
- If fetching topic counts fails, continue with `topics_count: 0`
- Logs the error for debugging but doesn't crash the request

### 4. Added Error Handling for Date Parsing
```python
exam_date_str = plan.get('exam_date')
if exam_date_str:
    exam_date = datetime.fromisoformat(exam_date_str).date()
    days_left = max(0, (exam_date - today).days)
else:
    days_left = 0
```
- Safely handles missing or invalid `exam_date` fields
- Defaults to `days_left: 0` if date is missing

### 5. Added Error Handling for Empty Plan IDs
```python
if plan_ids:  # Only query if we have plan IDs
    try:
        all_topics_response = sb_execute(...)
```
- Only attempts to fetch topic counts if there are plan IDs
- Prevents unnecessary queries and potential errors

### 6. Added Per-Plan Error Handling
```python
for plan in plans:
    try:
        # Format plan
    except Exception as plan_error:
        # Log error but continue with other plans
        continue
```
- If one plan has invalid data, skip it but continue processing others
- Prevents one bad plan from breaking the entire response

### 7. Enhanced Error Logging
```python
if ENABLE_DEBUG:
    import traceback
    print(f"❌ Error in get_study_plans: {error_msg}")
    print(traceback.format_exc())
```
- Logs full traceback in debug mode for easier troubleshooting
- Provides detailed error messages in production

## Testing

After deploying this fix, test the endpoint:

```bash
# Test with valid user_id
curl "https://imtehaanai-production.up.railway.app/api/v1/study-plans?user_id=YOUR_USER_ID"

# Expected response:
{
  "success": true,
  "data": [
    {
      "id": "...",
      "plan_name": "...",
      "subject_id": 101,
      "subject": "Business Studies",
      "topics_count": 5,
      "days_left": 30,
      "exam_date": "2024-02-15",
      "status": "active",
      "created_at": "..."
    }
  ]
}
```

## Next Steps

1. **Deploy the fix** to Railway
2. **Test the endpoint** with a real user_id
3. **Check Railway logs** for any remaining errors
4. **Monitor** the endpoint for 24 hours to ensure stability

## Additional Notes

- The endpoint now gracefully handles missing data
- Topic counts default to 0 if they can't be fetched
- Date calculations default to 0 days left if dates are missing
- All errors are logged for debugging but don't crash the request
- The endpoint returns an empty array if no plans are found (not an error)

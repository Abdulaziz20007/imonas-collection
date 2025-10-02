# Data Optimization System Cleanup - Changes Summary

## Overview

Simplified the data optimization cleanup system by removing `OPTIMIZATION_TIME_MINUTES` and keeping only `DATA_RETENTION_DAYS` configuration.

## Changes Made

### 1. **src/config.py**

- ❌ **Removed:** `OPTIMIZATION_TIME_MINUTES` configuration
- ✅ **Kept:** `DATA_RETENTION_DAYS` (default: 60 days, your .env: 90 days)
- All other cleanup configurations remain unchanged:
  - `CLEANUP_BATCH_SIZE` - How many records to process at once (default: 50)
  - `CLEANUP_BATCH_DELAY_SECONDS` - Delay between batches (default: 2 seconds)
  - `CLEANUP_DRY_RUN` - Test mode without actual deletion (default: false)

### 2. **src/services/data_optimizer_service.py**

- ❌ **Removed:** `self.age_threshold_minutes` field
- ❌ **Removed:** Complex dual-logic for minutes vs days
- ✅ **Simplified:** Now uses only `DATA_RETENTION_DAYS` to calculate cutoff
- ✅ **Simplified:** `_refresh_config()` method - removed OPTIMIZATION_TIME logic
- ✅ **Updated:** Log messages to reflect retention_days only

**Before (Complex):**

```python
if minutes and minutes > 0:
    cutoff_dt = datetime.utcnow() - timedelta(minutes=minutes)
    cutoff_basis = f"minutes={minutes}"
else:
    cutoff_dt = datetime.utcnow() - timedelta(days=int(self.retention_days))
    cutoff_basis = f"retention_days={self.retention_days}"
```

**After (Simple):**

```python
cutoff_dt = datetime.utcnow() - timedelta(days=int(self.retention_days))
```

### 3. **src/services/concurrent_integration.py**

- ✅ **Updated:** Comment from "uses OPTIMIZATION_TIME window" to "uses DATA_RETENTION_DAYS"

## How It Works Now

### Cleanup Schedule

The system runs cleanup in **2 places**:

1. **Daily scheduled job** (app.py):

   - Runs at 00:00 Asia/Tashkent time
   - Scheduled via Telegram bot's job queue
   - Full cleanup of all stale data

2. **Background task** (concurrent_integration.py):
   - Runs every 60 seconds
   - Continuous cleanup for real-time maintenance

### What Gets Cleaned Up

Data older than **90 days** (from your .env `DATA_RETENTION_DAYS=90`):

1. **Orders** - Old order records and associated files
2. **Product Media** - Videos, images, and thumbnails
3. **Payments** - Payment records and receipt images
4. **Reports** - User reports and PDF files
5. **Transactions** - Unlinked transaction records
6. **Collections** - Collections with no dependencies

### Batch Processing

- Processes **50 items** at a time (configurable via `CLEANUP_BATCH_SIZE`)
- Waits **2 seconds** between batches (configurable via `CLEANUP_BATCH_DELAY_SECONDS`)
- This prevents system overload and maintains responsiveness

### Safety Features

- **Dry Run Mode**: Set `CLEANUP_DRY_RUN=true` in .env to test without deleting
- **Logging**: All cleanup operations are logged for auditing
- **Gradual Processing**: Batch processing ensures system stability
- **Database-First**: Deletes DB records first, then cleans up files

## Your Current Configuration

From your `.env` file:

```env
DATA_RETENTION_DAYS=90  # Keep data for 3 months
```

This means:

- ✅ Data older than 90 days will be deleted
- ✅ Recent data (< 90 days) is safe
- ✅ Cleanup runs daily at midnight + every 60 seconds in background
- ✅ Safe batch processing (50 items at a time, 2 second delays)

## Testing Recommendations

### 1. Enable Dry Run Mode First

Add to your `.env`:

```env
CLEANUP_DRY_RUN=true
```

This will show what would be deleted without actually deleting anything.

### 2. Check Logs

Look for log messages like:

```
Starting cleanup cycle (cutoff<2025-01-03 00:00:00, retention_days=90, batch_size=50, delay=2s, dry_run=true)
[DRY RUN] Would delete 50 rows from orders: [1, 2, 3, ...]
Cleanup cycle finished. Deleted counts (dry_run=true): orders=150, media=75, payments=200, ...
```

### 3. When Ready for Real Cleanup

Set in `.env`:

```env
CLEANUP_DRY_RUN=false
```

## Benefits of This Change

1. ✅ **Simpler Configuration** - Only one setting to manage
2. ✅ **Clearer Logic** - No confusing dual-mode behavior
3. ✅ **Easier to Understand** - "Keep data for X days" is intuitive
4. ✅ **Less Error-Prone** - No conflicting time settings
5. ✅ **Better for Long-term Retention** - Days are better unit for data retention

## Files Modified

1. `src/config.py` - Removed OPTIMIZATION_TIME_MINUTES
2. `src/services/data_optimizer_service.py` - Simplified to use only DATA_RETENTION_DAYS
3. `src/services/concurrent_integration.py` - Updated comment

## No Breaking Changes

- Database schema unchanged
- API unchanged
- All existing cleanup functionality preserved
- Only configuration simplified

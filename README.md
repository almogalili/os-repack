# os-repack
PowerShell automation for safely re-indexing and shrinking OpenSearch time-series indexes.  
Preserves routing, retention (ISM), and aliases while re-creating the index with fewer primary shards.  
Supports async reindex, throttling, custom mappings, compression, replica management, and field removal.

---

## Features
- Retention-aware: reads `delete → min_index_age` from ISM policy and computes remaining retention.
- Safe: refuses to run on alias write index.
- Preserves routing (`routing = keep`) when required.
- Removes specified fields during reindex.
- Supports async reindex with `wait_for_completion=false`.
- Supports throttling via `requests_per_second`.
- Supports compression (`best_compression` or `default`).
- Disables replicas during reindex, re-enables them afterward.

---

## Requirements
- OpenSearch or AWS OpenSearch Service  
- PowerShell 7+ (Windows PowerShell 5.1 compatible)  
- User with permissions to read/write indices and ISM policies

---

## Example Usage
```powershell
.\repack.ps1 `
  -OsUrl "https://your-domain.us-east-1.es.amazonaws.com" `
  -Username "admin" `
  -Password "********" `
  -SourceIndex "logs-000123" `
  -TargetPrimaryShards 1 `
  -TargetReplicas 1 `
  -AliasName "logs-" `
  -RepackPrefix "repack-" `
  -FieldsToRemove @("debug_field","temp_data") `
  -Compression "best_compression" `
  -Async `
  -RequestsPerSecond 500 `
  -PollSeconds 10 `
  -MaxWaitMinutes 720
```

---

## Workflow
1. Validates that the source index is not the alias write index.  
2. Reads ISM policy and calculates remaining retention time.  
3. Creates new target index with fewer primary shards and compression.  
4. Reindexes data while removing specified fields.  
5. Restores replicas after reindexing.  
6. Creates a per-index ISM delete policy using remaining retention.  
7. Updates alias: removes old index, adds new index (non-writer).

---

## Example Output
```
DEBUG: delete→min_index_age from 'logs-rollover' is 50m
Retention DEBUG → policy=logs-rollover | orig_age=50m | created=2025-11-08 15:58Z | deadline=2025-11-08 16:48Z | remaining=47m
Created index repack-logs-000003
Started async reindex task: APXUbyXzTO6VORsfGQkgKQ:14722458
Replicas restored on repack-logs-000003.
Attached policy retain-repack-logs-000003-20251108180124
Alias 'logs-' updated.
Repack Completed — Source: logs-000003 → Target: repack-logs-000003
```

---

## Notes
- Should only be used on read-only or closed indexes.
- Reindexing is resource-intensive; monitor cluster health.
- Safe to rerun — delete target index if it already exists.

---


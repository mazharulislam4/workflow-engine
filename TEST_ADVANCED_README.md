# Advanced Workflow Test - Summary

## Test Coverage

This test demonstrates all advanced workflow features:

### ✅ Features Tested

1. **Fork (Parallel Execution)**

   - Main fork with 3 parallel paths
   - Wait for completion mode
   - Thread-based execution

2. **Nested Fork (Fork inside Fork)**

   - Secondary fork inside first fork path
   - 2 nested paths executing in parallel
   - Proper synchronization

3. **Loop (Iteration)**

   - Iterates over arrays
   - Executes child nodes for each item
   - 3 iterations with HTTP requests

4. **Condition (Branching Logic)**

   - Evaluates expressions: `{{steps.*.outputs}} == 200`
   - True/false path routing
   - Multiple conditions in workflow

5. **Parallel Execution**

   - Multiple nodes at same dependency level
   - Concurrent HTTP requests
   - Proper level-by-level execution

6. **Template Variables (Step-to-Step Data Flow)**
   - `{{variables.*}}` - workflow variables
   - `{{steps.*.outputs.*}}` - cross-step references
   - `{{loop.item}}`, `{{loop.index}}` - loop context
   - Dynamic URL construction

## Workflow Structure

```
Start
  └─> Fetch Users (HTTP)
       └─> Check Users Exist (Condition)
            ├─> TRUE: Loop Process Users
            │    └─> Fork Parallel Operations
            │         ├─> Path: Fetch Posts
            │         │    └─> Get Posts
            │         │         └─> Check Posts Count (Condition)
            │         │              ├─> Many: Process Posts Success
            │         │              └─> Few: Process Posts Fallback
            │         ├─> Path: Fetch Comments
            │         │    └─> Get Comments
            │         └─> Nested Fork
            │              ├─> Path: Albums
            │              │    └─> Get Albums
            │              │         └─> Loop Albums (3 iterations)
            │              └─> Path: Photos
            │                   └─> Get Photos
            └─> FALSE: Failure Notification

All paths converge at:
  Merge Results
    └─> Final Condition
         ├─> TRUE: Success Notification
         └─> FALSE: Failure Notification
              └─> End
```

## Configuration

- **Workflow Level Timeout**: 3600s (1 hour)
- **Fork Timeout**: 300s (5 minutes)
- **Path Level Timeout**: 120s (2 minutes)
- **Node Timeouts**: 10-30s per HTTP request
- **Execution Mode**: Thread-based
- **Max Workers**: 5 for main fork, 3 for nested fork

## Template Examples

### Variable Substitution

```json
"url": "{{variables.api_base}}/users"
→ "https://jsonplaceholder.typicode.com/users"
```

### Step Output Reference

```json
"expression": "{{steps.fetch_users.outputs.status_code}} == 200"
→ "200 == 200"
```

### Loop Context

```json
"url": "{{variables.api_base}}/users/{{loop.item}}"
→ For item=1: "https://jsonplaceholder.typicode.com/users/1"
```

### Conditional Logic

```json
"expression": "{{steps.get_posts.outputs.result|length}} > {{variables.data_threshold}}"
→ "100 > 5" → TRUE
```

## Running the Test

```bash
python test_advanced_workflow.py
```

## Expected Output

```
============================================================
  ADVANCED WORKFLOW TEST
============================================================
Testing: Fork, Loop, Condition, Parallel, Nested Fork, Templates

[*] Workflow Configuration:
   - Name: Advanced Workflow Test
   - Version: 3.0.0
   - Total Nodes: 23
   - Total Edges: 28
   - Timeout: 3600s

[*] Starting workflow execution...

============================================================
  EXECUTION RESULTS
============================================================

[SUCCESS] Workflow completed successfully!

[*] Key Metrics:
   - Nodes executed: 20+
   - Loop iterations: 3
   - Fork paths executed: 3/3
   - Nested fork paths: 2/2

[*] Template Variable Usage Examples:
   [+] Variable substitution: API call returned status 200
   [+] Loop variables: Processed user IDs using {{loop.item}}
   [+] Condition with templates: Evaluated posts count (100)
   [+] Cross-step references: Used {{steps.*.outputs}} for merging

============================================================
  TEST SUMMARY
============================================================
[SUCCESS] All features tested successfully:
   [+] Fork (parallel execution)
   [+] Nested Fork (fork inside fork)
   [+] Loop (iteration with loop variables)
   [+] Condition (branching logic)
   [+] Parallel execution (multiple paths)
   [+] Template variables (step-to-step data flow)

[*] Test completed!
```

## Notes

- Clean output with minimal logging (WARNING level for libraries)
- Uses real API calls to jsonplaceholder.typicode.com
- Tests actual parallel execution and synchronization
- Demonstrates proper timeout configuration at all levels
- Shows template variable evaluation across different contexts

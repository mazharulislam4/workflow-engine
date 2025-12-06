# Comprehensive Workflow Test Structure

## Overview

This workflow tests all major features: Fork nodes, Nested forks, Conditions, Parallel execution, and Error routing.

## Workflow Diagram

```
START
  ↓
initial_http (HTTP Request)
  ↓ [success]
check_initial_status (Condition: status == 200)
  ↓ [true]                     ↓ [false]
fork_main (FORK)               error_path
  ├─→ path_1                     ↓
  │     ↓                       END
  │   path1_http_1
  │     ↓
  │   path1_http_2
  │
  ├─→ path_2 (Parallel inside path)
  │     ├─→ path2_http_1
  │     ├─→ path2_http_2
  │     └─→ path2_http_3
  │
  └─→ path_3
        ↓
      nested_fork (NESTED FORK)
        ├─→ nested_path_1
        │     ↓
        │   nested_path1_http_1
        │
        └─→ nested_path_2
              ↓
            nested_path2_http_1
              ↓
            nested_path2_condition (Condition)
              ↓ [true]              ↓ [false]
            nested_path2_http_success  nested_path2_http_error

[After all fork paths complete]
  ↓
after_fork (HTTP Request - aggregates results)
  ↓ [success]
final_condition (Condition: status == 200)
  ↓ [true]              ↓ [false]
success_path          error_path
  ↓                     ↓
END ←─────────────────┘
```

## Node Types

### Start/End Nodes

- `start`: Workflow entry point
- `end`: Workflow completion point

### HTTP Request Nodes

- `initial_http`: First HTTP request to validate workflow start
- `path1_http_1`, `path1_http_2`: Sequential requests in Path 1
- `path2_http_1`, `path2_http_2`, `path2_http_3`: Parallel requests in Path 2
- `nested_path1_http_1`: HTTP request in nested path 1
- `nested_path2_http_1`: HTTP request in nested path 2
- `nested_path2_http_success`: Success branch in nested condition
- `nested_path2_http_error`: Error branch in nested condition
- `after_fork`: Aggregates fork results
- `success_path`: Final success request
- `error_path`: Final error/fallback request

### Condition Nodes

- `check_initial_status`: Validates initial HTTP response
- `nested_path2_condition`: Condition inside nested fork
- `final_condition`: Final workflow validation

### Fork Nodes

- `fork_main`: Main fork with 3 parallel paths
- `nested_fork`: Nested fork inside path_3 (fork within fork)

### Path Nodes

- `path_1`: Simple sequential execution
- `path_2`: Parallel execution (fanout pattern)
- `path_3`: Contains nested fork
- `nested_path_1`: Simple path in nested fork
- `nested_path_2`: Path with condition in nested fork

## Test Scenarios

### 1. Fork with Multiple Paths

- **path_1**: Tests sequential execution (2 HTTP requests in series)
- **path_2**: Tests parallel execution (3 HTTP requests simultaneously)
- **path_3**: Tests nested fork (fork inside a path)

### 2. Nested Fork (Fork Inside Fork)

- Main fork → path_3 → nested_fork → nested paths
- Tests multi-level parallelism
- Validates fork isolation and result aggregation

### 3. Parallel Execution Within Path

- path_2 has 3 HTTP nodes with no dependencies
- All 3 should execute in parallel (same level)
- Tests fanout pattern

### 4. Condition-Based Branching

- **check_initial_status**: Routes to fork or error path
- **nested_path2_condition**: Routes within nested fork
- **final_condition**: Routes to success or error final request

### 5. Error Routing

- Success edges: Normal execution flow
- Error edges: Alternative paths on failure
- All HTTP nodes have proper error handling

## Expected Behavior

### Path Execution Order (Levels)

```
Level 0: start
Level 1: initial_http
Level 2: check_initial_status
Level 3: fork_main
Level 4: path_1, path_2, path_3 (parallel)
  - Path 1:
    Level 4a: path1_http_1
    Level 4b: path1_http_2
  - Path 2:
    Level 4a: path2_http_1, path2_http_2, path2_http_3 (parallel)
  - Path 3:
    Level 4a: nested_fork
    Level 4b: nested_path_1, nested_path_2 (parallel)
      - Nested Path 1:
        Level 4c: nested_path1_http_1
      - Nested Path 2:
        Level 4c: nested_path2_http_1
        Level 4d: nested_path2_condition
        Level 4e: nested_path2_http_success OR nested_path2_http_error
Level 5: after_fork
Level 6: final_condition
Level 7: success_path OR error_path
Level 8: end
```

### Fork Result Structure

```json
{
  "type": "fork",
  "total_paths": 3,
  "paths_executed": 3,
  "paths": {
    "path_1": {
      "condition_met": true,
      "nodes": {
        "path1_http_1": { "status": "success", "output": {...} },
        "path1_http_2": { "status": "success", "output": {...} }
      },
      "status": "success"
    },
    "path_2": {
      "condition_met": true,
      "nodes": {
        "path2_http_1": { "status": "success", "output": {...} },
        "path2_http_2": { "status": "success", "output": {...} },
        "path2_http_3": { "status": "success", "output": {...} }
      },
      "status": "success"
    },
    "path_3": {
      "condition_met": true,
      "nodes": {
        "nested_fork": {
          "type": "fork",
          "total_paths": 2,
          "paths_executed": 2,
          "paths": {
            "nested_path_1": {...},
            "nested_path_2": {...}
          }
        }
      },
      "status": "success"
    }
  }
}
```

## Variables Used

- `api_posts_1`, `api_posts_2`, `api_posts_3`: JSONPlaceholder posts API
- `api_users_1`, `api_users_2`, `api_users_3`: JSONPlaceholder users API
- `api_comments_1`, `api_comments_2`: JSONPlaceholder comments API
- `api_albums_1`: JSONPlaceholder albums API
- `api_photos_1`: JSONPlaceholder photos API
- `success_threshold`: HTTP status code threshold (200)

## Running the Test

```bash
python test_comprehensive.py
```

Expected output:

- All HTTP requests should succeed (200 status)
- All paths should execute
- Nested fork should complete
- Final condition should route to success_path
- Workflow status: COMPLETED

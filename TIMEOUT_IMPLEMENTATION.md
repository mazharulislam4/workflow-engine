# Timeout Implementation

This document describes the comprehensive timeout functionality implemented across the workflow execution system.

## Overview

Timeout support has been added at three levels:

1. **Workflow Level** - Controls overall workflow execution time
2. **Node Level** - Controls individual node execution time
3. **Path/Fork Level** - Controls parallel execution of downstream nodes

## Timeout Levels

### 1. Workflow-Level Timeout

Controls the maximum time allowed for executing each dependency level in the workflow.

**Configuration:**

```json
{
  "id": "my-workflow",
  "name": "My Workflow",
  "config": {
    "level_timeout": 300  // 5 minutes (default: 300 seconds)
  },
  "nodes": [...],
  "edges": [...]
}
```

**Implementation:**

- Located in: `workflow/engine/executor.py`
- Applied in: `WorkflowExecutor._execute_level_parallel()`
- Timeout applies to each dependency level (parallel execution of nodes at same level)
- If timeout is exceeded, remaining futures are cancelled and `TimeoutError` is raised

**Example:**

```python
workflow_def = {
    "config": {
        "level_timeout": 600  # 10 minutes per level
    },
    # ... rest of workflow
}

executor = WorkflowExecutor(run_id, workflow_def)
result = executor.execute()  # Will timeout if any level takes > 10 min
```

### 2. Node-Level Timeout

Controls the maximum time allowed for a single node to execute (including retries).

**Configuration:**

```json
{
  "id": "api_call_node",
  "type": "http_request",
  "config": {
    "url": "https://api.example.com/data",
    "timeout": 30, // 30 seconds (default: None - no timeout)
    "retry": {
      "max_retries": 3,
      "delay_seconds": 1
    }
  }
}
```

**Implementation:**

- Located in: `workflow/executors/base.py`
- Applied in: `NodeExecutor._execute_with_retries()`
- Uses `ThreadPoolExecutor` with `future.result(timeout=node_timeout)`
- Timeout applies to each retry attempt
- If timeout is exceeded, `TimeoutError` is raised

**Important Notes:**

- Node timeout is checked AFTER inputs are prepared via `_prepare_inputs()`
- Timeout applies to the `execute_adapter()` method execution
- Works with retry logic - each retry attempt has the full timeout duration

**Example:**

```json
{
  "id": "slow_api",
  "type": "http_request",
  "config": {
    "url": "https://slow-api.com/endpoint",
    "timeout": 45, // Max 45 seconds per attempt
    "retry": {
      "max_retries": 2
    }
  }
}
```

### 3. Path-Level Timeout

Controls the maximum time allowed for executing all downstream nodes of a path in parallel.

**Configuration:**

```json
{
  "id": "path_1",
  "type": "path",
  "config": {
    "condition": "{{variables.enable_path}}",
    "level_timeout": 300 // 5 minutes (default: 300 seconds)
  }
}
```

**Implementation:**

- Located in: `workflow/executors/path.py`
- Applied in: `PathNodeExecutor._execute_level()`
- Timeout applies to parallel execution of nodes at each dependency level within the path
- Uses `as_completed(futures, timeout=level_timeout)`
- If timeout is exceeded, remaining futures are cancelled

**Example:**

```json
{
  "id": "conditional_path",
  "type": "path",
  "config": {
    "condition": "{{steps.http_1.outputs.status_code}} == 200",
    "level_timeout": 180 // 3 minutes for downstream execution
  }
}
```

### 4. Fork-Level Timeout

Controls the maximum time allowed for executing all fork paths in parallel.

**Configuration:**

```json
{
  "id": "fork_node",
  "type": "fork",
  "config": {
    "max_workers": 5,
    "timeout": 600, // 10 minutes (default: 600 seconds)
    "max_nodes_per_path": 50,
    "max_total_nodes": 200
  }
}
```

**Implementation:**

- Located in: `workflow/executors/fork.py`
- Applied in: `ForkNodeExecutor._execute_path_thread()`
- Timeout applies to parallel execution of all fork paths
- Uses `as_completed(futures, timeout=fork_timeout)`
- If timeout is exceeded, remaining path executions are cancelled

**Example:**

```json
{
  "id": "parallel_processing",
  "type": "fork",
  "config": {
    "max_workers": 10,
    "timeout": 900, // 15 minutes for all paths
    "max_nodes_per_path": 100
  }
}
```

## Timeout Behavior

### When Timeout Occurs

1. **Node Timeout**:

   - `TimeoutError` is raised
   - Node is marked as failed
   - Respects `continue_on_error` configuration
   - Retry logic is interrupted

2. **Path Timeout**:

   - All pending node executions in that level are cancelled
   - `TimeoutError` is raised
   - Path condition is marked as executed but with error

3. **Fork Timeout**:

   - All pending path executions are cancelled
   - `TimeoutError` is raised
   - Partially completed paths are recorded in results

4. **Workflow Timeout**:
   - All pending node executions at that level are cancelled
   - `TimeoutError` is raised
   - Workflow execution stops

### Error Handling with Timeouts

Timeouts respect error handling configurations:

```json
{
  "id": "risky_node",
  "type": "http_request",
  "config": {
    "url": "{{variables.api_url}}",
    "timeout": 30,
    "error_handling": {
      "continue_on_error": true // Timeout won't fail workflow
    }
  }
}
```

## Timeout Hierarchy

The timeout hierarchy from outermost to innermost:

```
Workflow Level Timeout (per level)
  └─> Fork Level Timeout (all paths)
       └─> Path Level Timeout (per path level)
            └─> Node Level Timeout (individual node)
```

**Example Scenario:**

- Workflow level_timeout: 600s (10 min per dependency level)
- Fork timeout: 300s (5 min for all fork paths)
- Path level_timeout: 120s (2 min per path level)
- Node timeout: 30s (30 sec per node)

This ensures:

1. No node takes more than 30 seconds
2. No path level takes more than 2 minutes
3. No fork takes more than 5 minutes
4. No workflow level takes more than 10 minutes

## Default Values

| Level    | Default Timeout   | Configuration Key      |
| -------- | ----------------- | ---------------------- |
| Workflow | 300s (5 min)      | `config.level_timeout` |
| Node     | None (no timeout) | `config.timeout`       |
| Path     | 300s (5 min)      | `config.level_timeout` |
| Fork     | 600s (10 min)     | `config.timeout`       |

## Implementation Details

### Why Input Preparation Happens Before Timeout

You correctly identified that inputs must be prepared before the timeout check. This is because:

1. **Template Evaluation**: Node configs contain templates like `{{variables.api_url}}` that must be resolved from context
2. **Context Access**: The `_prepare_inputs()` method accesses workflow context, evaluates expressions, and builds the input dictionary
3. **Node Dependencies**: Input preparation may depend on outputs from previous nodes

The execution flow is:

```python
def run(self):
    # ... pre-execution checks ...
    result = self._execute_with_retries()  # Timeout happens here

def _execute_with_retries(self):
    # Timeout wraps the execution
    if node_timeout:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.execute_adapter)  # <-- Timeout starts here
            result = future.result(timeout=node_timeout)

def execute_adapter(self):
    inputs = self._prepare_inputs()  # <-- Inputs prepared inside timeout
    return self.execute(inputs)
```

### Concurrent Execution with Timeouts

All parallel execution uses `concurrent.futures.as_completed()` with timeout:

```python
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = {executor.submit(task, arg): arg for arg in args}

    try:
        for future in as_completed(futures, timeout=timeout_value):
            result = future.result()
            # Process result
    except FuturesTimeoutError:
        # Cancel remaining futures
        for future in futures:
            future.cancel()
        raise TimeoutError(f"Execution timeout ({timeout_value}s) exceeded")
```

## Testing Timeouts

### Test Node Timeout

```json
{
  "id": "slow_node",
  "type": "http_request",
  "config": {
    "url": "https://httpbin.org/delay/60", // Takes 60 seconds
    "timeout": 5, // Will timeout after 5 seconds
    "error_handling": {
      "continue_on_error": true
    }
  }
}
```

### Test Path Timeout

```json
{
  "id": "slow_path",
  "type": "path",
  "config": {
    "condition": true,
    "level_timeout": 10 // 10 second timeout for downstream
  }
}
```

### Test Fork Timeout

```json
{
  "id": "slow_fork",
  "type": "fork",
  "config": {
    "timeout": 30, // 30 second timeout for all paths
    "max_workers": 5
  }
}
```

## Logging

Timeout events are logged with clear messages:

```
ERROR: Node slow_api execution timeout (30s) exceeded
ERROR: Path level execution timeout (300s) exceeded
ERROR: Fork execution timeout (600s) exceeded
ERROR: Workflow level execution timeout (300s) exceeded
```

## Best Practices

1. **Set Realistic Timeouts**: Consider network latency, processing time, and retry attempts
2. **Use Hierarchy**: Set tighter timeouts at lower levels, looser at higher levels
3. **Error Handling**: Use `continue_on_error` for non-critical nodes with timeouts
4. **Monitor Logs**: Watch for timeout errors to tune timeout values
5. **Test Thoroughly**: Test with actual workloads to determine appropriate timeout values

## Troubleshooting

### Timeout Too Short

**Symptom**: Nodes consistently timing out  
**Solution**: Increase timeout value or optimize node execution

### Timeout Too Long

**Symptom**: Workflow hangs for extended periods  
**Solution**: Decrease timeout value for faster failure detection

### Cascading Timeouts

**Symptom**: Multiple levels timing out simultaneously  
**Solution**: Review timeout hierarchy, ensure each level has appropriate timeout relative to children

## Migration Guide

To add timeouts to existing workflows:

1. **Identify Slow Nodes**: Review execution logs to find slow-running nodes
2. **Add Node Timeouts**: Start with generous timeouts (2-3x typical execution time)
3. **Add Path/Fork Timeouts**: Set to accommodate all downstream nodes
4. **Add Workflow Timeout**: Set based on total expected workflow duration
5. **Monitor and Adjust**: Review logs and adjust timeouts based on actual performance

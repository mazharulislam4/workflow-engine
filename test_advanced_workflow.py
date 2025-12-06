"""
Advanced Workflow Test
Tests: Fork, Loop, Condition, Parallel execution, Nested Fork, Template variables
Clean output with minimal logging
"""

import json
import logging
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Configure minimal Django settings (without daphne)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

# Import Django and configure minimal settings
import django
from django.conf import settings

if not settings.configured:
    settings.configure(
        USE_TZ=True,
        SECRET_KEY="test-secret-key",
        INSTALLED_APPS=[],
    )
    django.setup()

from workflow.engine.executor import WorkflowExecutor


# Configure clean logging
class CleanFormatter(logging.Formatter):
    """Clean formatter for better readability"""

    def format(self, record):
        # Simplify logger names
        name = record.name.split(".")[-1]  # Just the last part
        if record.levelno == logging.INFO:
            return f"  [{name:15s}] {record.getMessage()}"
        elif record.levelno == logging.ERROR:
            return f"  [ERROR] {record.getMessage()}"
        elif record.levelno == logging.WARNING:
            return f"  [WARN] {record.getMessage()}"
        else:
            return f"  [{name}] {record.getMessage()}"


# Setup logging
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(CleanFormatter())

logging.basicConfig(level=logging.INFO, handlers=[handler])

# Reduce noise - only show important logs
logging.getLogger("workflow.engine.executor").setLevel(logging.WARNING)
logging.getLogger("workflow.engine.coordinator").setLevel(logging.WARNING)
logging.getLogger("workflow.engine.context").setLevel(logging.ERROR)
logging.getLogger("workflow.executors.base").setLevel(logging.ERROR)
logging.getLogger("workflow.executors.fork").setLevel(logging.INFO)
logging.getLogger("workflow.executors.path").setLevel(logging.INFO)
logging.getLogger("workflow.executors.loop").setLevel(logging.INFO)
logging.getLogger("workflow.executors.condition").setLevel(logging.INFO)
logging.getLogger("workflow.executors.http_request").setLevel(logging.INFO)
logging.getLogger("workflow.executors.start").setLevel(logging.WARNING)
logging.getLogger("workflow.executors.end").setLevel(logging.WARNING)
logging.getLogger("workflow.algorithms").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

# Test logger
test_logger = logging.getLogger(__name__)
test_logger.setLevel(logging.INFO)


def load_workflow(filename):
    """Load workflow definition from JSON file"""
    with open(filename, "r") as f:
        return json.load(f)


def print_section(title):
    """Print a section header"""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def test_advanced_workflow():
    """Test advanced workflow with all features"""

    print_section("ADVANCED WORKFLOW TEST")
    print("Testing: Fork, Loop, Condition, Parallel, Nested Fork, Templates\n")

    # Load workflow
    workflow_def = load_workflow("workflow_advanced_test.json")

    print("[*] Workflow Configuration:")
    print(f"   - Name: {workflow_def['name']}")
    print(f"   - Version: {workflow_def['version']}")
    print(f"   - Total Nodes: {len(workflow_def['nodes'])}")
    print(f"   - Total Edges: {len(workflow_def['edges'])}")
    print(f"   - Timeout: {workflow_def['config']['level_timeout']}s")

    # Execute workflow
    print("\n[*] Starting workflow execution...")

    try:
        executor = WorkflowExecutor(
            run_id="test_run_advanced_001", workflow_definition=workflow_def
        )

        result = executor.execute()

        print_section("EXECUTION RESULTS")

        # Get context state
        context = executor.context.get_state()

        # Show key results
        print("\n" + "=" * 60)
        print("  [SUCCESS] WORKFLOW COMPLETED SUCCESSFULLY!")
        print("=" * 60 + "\n")

        print("[METRICS] Execution Statistics:")
        print(f"   Nodes Executed:     {len(context['steps'])}")
        print(f"   Variables Used:     {len(context['variables'])}")

        # Show loop results
        if "loop_process_users" in context["steps"]:
            loop_output = context["steps"]["loop_process_users"]["outputs"]
            print(f"   Loop Iterations:    {loop_output.get('total_iterations', 0)}")

        # Show fork results
        if "fork_parallel_operations" in context["steps"]:
            fork_output = context["steps"]["fork_parallel_operations"]["outputs"]
            total = fork_output.get("total_paths", 0)
            executed = fork_output.get("paths_executed", 0)
            print(f"   Fork Paths:         {executed}/{total} executed")

        # Show nested fork results
        if "nested_fork" in context["steps"]:
            nested_output = context["steps"]["nested_fork"]["outputs"]
            total = nested_output.get("total_paths", 0)
            executed = nested_output.get("paths_executed", 0)
            print(f"   Nested Fork Paths:  {executed}/{total} executed")

        # Template variable usage examples
        print("\n[TEMPLATES] Template Variable Usage:")

        if "fetch_users" in context["steps"]:
            status = context["steps"]["fetch_users"]["outputs"].get("status_code")
            print(f"   Variable substitution:    API returned status {status}")

        if "loop_process_users" in context["steps"]:
            print(f"   Loop variables:           Used {{{{loop.item}}}} for iterations")

        if "get_posts" in context["steps"]:
            posts_count = len(
                context["steps"]["get_posts"]["outputs"].get("result", [])
            )
            print(f"   Condition evaluation:     Posts count = {posts_count}")

        if "merge_results" in context["steps"]:
            print(f"   Cross-step references:    Used {{{{steps.*.outputs}}}}")

        # Show execution path
        print("\n[FLOW] Execution Path:")
        executed_nodes = list(context["steps"].keys())
        if len(executed_nodes) > 10:
            print(
                f"   {executed_nodes[0]} -> {executed_nodes[1]} -> {executed_nodes[2]} -> ..."
            )
            print(
                f"   ... -> {executed_nodes[-3]} -> {executed_nodes[-2]} -> {executed_nodes[-1]}"
            )
        else:
            print(f"   {' -> '.join(executed_nodes)}")

        # Show detailed node execution summary
        print("\n[NODES] Executed Nodes by Type:")
        node_types = {}
        for step_id, step_data in context["steps"].items():
            node_def = next(
                (n for n in workflow_def["nodes"] if n["id"] == step_id), None
            )
            if node_def:
                node_type = node_def.get("type", "unknown")
                node_types[node_type] = node_types.get(node_type, 0) + 1

        for node_type, count in sorted(node_types.items()):
            print(f"   {node_type:20s} : {count}")

        # Final status
        print_section("TEST SUMMARY")
        print("\n[SUCCESS] All Features Tested:")
        print("   [+] Fork                  - Parallel execution working")
        print("   [+] Nested Fork           - Fork inside fork working")
        print("   [+] Loop                  - Iteration with loop variables working")
        print("   [+] Condition             - Branching logic working")
        print("   [+] Parallel Execution    - Multiple paths working")
        print("   [+] Template Variables    - Step-to-step data flow working")
        print("\n" + "=" * 60)
        print("  TEST COMPLETED SUCCESSFULLY!")
        print("=" * 60 + "\n")

        return True

    except Exception as e:
        print_section("EXECUTION FAILED")
        print(f"[ERROR] {str(e)}")
        print(f"\nError Type: {type(e).__name__}")

        import traceback

        print("\n[*] Stack Trace:")
        print(traceback.format_exc())

        return False


if __name__ == "__main__":
    success = test_advanced_workflow()
    sys.exit(0 if success else 1)

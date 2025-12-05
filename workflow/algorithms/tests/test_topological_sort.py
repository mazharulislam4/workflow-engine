"""
Unit tests for topological sort algorithms
"""

import sys
import unittest
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from workflow.algorithms.topological_sort import (
    compute_dependency_levels,
    get_parallel_levels,
    group_nodes_by_level,
    topological_sort,
)


class TestTopologicalSort(unittest.TestCase):
    """Test cases for topological_sort function"""

    def test_simple_linear_graph(self):
        """Test simple linear workflow"""
        nodes = [
            {"id": "start"},
            {"id": "a"},
            {"id": "b"},
            {"id": "end"},
        ]
        edges = [
            {"source": "start", "target": "a"},
            {"source": "a", "target": "b"},
            {"source": "b", "target": "end"},
        ]

        result = topological_sort(nodes, edges)
        self.assertEqual(result, ["start", "a", "b", "end"])

    def test_branching_graph(self):
        """Test workflow with branches"""
        nodes = [
            {"id": "start"},
            {"id": "a"},
            {"id": "b"},
            {"id": "c"},
            {"id": "end"},
        ]
        edges = [
            {"source": "start", "target": "a"},
            {"source": "a", "target": "b"},
            {"source": "a", "target": "c"},
            {"source": "b", "target": "end"},
            {"source": "c", "target": "end"},
        ]

        result = topological_sort(nodes, edges)

        # Check that start comes first and end comes last
        self.assertEqual(result[0], "start")
        self.assertEqual(result[-1], "end")

        # Check that 'a' comes before 'b' and 'c'
        idx_a = result.index("a")
        idx_b = result.index("b")
        idx_c = result.index("c")
        self.assertLess(idx_a, idx_b)
        self.assertLess(idx_a, idx_c)

    def test_single_node(self):
        """Test single node workflow"""
        nodes = [{"id": "single"}]
        edges = []

        result = topological_sort(nodes, edges)
        self.assertEqual(result, ["single"])

    def test_parallel_branches(self):
        """Test workflow with parallel branches"""
        nodes = [
            {"id": "start"},
            {"id": "branch1"},
            {"id": "branch2"},
            {"id": "branch3"},
            {"id": "end"},
        ]
        edges = [
            {"source": "start", "target": "branch1"},
            {"source": "start", "target": "branch2"},
            {"source": "start", "target": "branch3"},
            {"source": "branch1", "target": "end"},
            {"source": "branch2", "target": "end"},
            {"source": "branch3", "target": "end"},
        ]

        result = topological_sort(nodes, edges)

        # Start must be first, end must be last
        self.assertEqual(result[0], "start")
        self.assertEqual(result[-1], "end")

        # All branches should come before end
        idx_end = result.index("end")
        self.assertLess(result.index("branch1"), idx_end)
        self.assertLess(result.index("branch2"), idx_end)
        self.assertLess(result.index("branch3"), idx_end)

    def test_cycle_detection(self):
        """Test that cycles are detected"""
        nodes = [
            {"id": "a"},
            {"id": "b"},
            {"id": "c"},
        ]
        edges = [
            {"source": "a", "target": "b"},
            {"source": "b", "target": "c"},
            {"source": "c", "target": "a"},  # Creates cycle
        ]

        with self.assertRaises(ValueError) as cm:
            topological_sort(nodes, edges)

        self.assertIn("Cycle detected", str(cm.exception))


class TestGetParallelLevels(unittest.TestCase):
    """Test cases for get_parallel_levels function"""

    def test_simple_linear_levels(self):
        """Test linear workflow levels"""
        nodes = [
            {"id": "start"},
            {"id": "a"},
            {"id": "b"},
            {"id": "end"},
        ]
        edges = [
            {"source": "start", "target": "a"},
            {"source": "a", "target": "b"},
            {"source": "b", "target": "end"},
        ]

        levels = get_parallel_levels(nodes, edges)

        self.assertEqual(len(levels), 4)
        self.assertEqual(levels[0], ["start"])
        self.assertEqual(levels[1], ["a"])
        self.assertEqual(levels[2], ["b"])
        self.assertEqual(levels[3], ["end"])

    def test_parallel_branches_levels(self):
        """Test parallel branches are in same level"""
        nodes = [
            {"id": "start"},
            {"id": "api1"},
            {"id": "api2"},
            {"id": "api3"},
            {"id": "join"},
            {"id": "end"},
        ]
        edges = [
            {"source": "start", "target": "api1"},
            {"source": "start", "target": "api2"},
            {"source": "start", "target": "api3"},
            {"source": "api1", "target": "join"},
            {"source": "api2", "target": "join"},
            {"source": "api3", "target": "join"},
            {"source": "join", "target": "end"},
        ]

        levels = get_parallel_levels(nodes, edges)

        self.assertEqual(len(levels), 4)
        self.assertEqual(levels[0], ["start"])
        self.assertCountEqual(levels[1], ["api1", "api2", "api3"])
        self.assertEqual(levels[2], ["join"])
        self.assertEqual(levels[3], ["end"])

    def test_complex_branching_levels(self):
        """Test complex workflow with multiple branch points"""
        nodes = [
            {"id": "start"},
            {"id": "a"},
            {"id": "b"},
            {"id": "c"},
            {"id": "end"},
        ]
        edges = [
            {"source": "start", "target": "a"},
            {"source": "a", "target": "b"},
            {"source": "a", "target": "c"},
            {"source": "b", "target": "end"},
            {"source": "c", "target": "end"},
        ]

        levels = get_parallel_levels(nodes, edges)

        self.assertEqual(len(levels), 4)
        self.assertEqual(levels[0], ["start"])
        self.assertEqual(levels[1], ["a"])
        self.assertCountEqual(levels[2], ["b", "c"])
        self.assertEqual(levels[3], ["end"])

    def test_empty_graph(self):
        """Test empty graph"""
        nodes = []
        edges = []

        levels = get_parallel_levels(nodes, edges)
        self.assertEqual(levels, [])

    def test_single_node_level(self):
        """Test single node"""
        nodes = [{"id": "single"}]
        edges = []

        levels = get_parallel_levels(nodes, edges)
        self.assertEqual(levels, [["single"]])


class TestComputeDependencyLevels(unittest.TestCase):
    """Test cases for compute_dependency_levels function"""

    def test_simple_linear_dependency_levels(self):
        """Test linear workflow dependency levels"""
        nodes = [
            {"id": "start"},
            {"id": "a"},
            {"id": "b"},
            {"id": "end"},
        ]
        edges = [
            {"source": "start", "target": "a"},
            {"source": "a", "target": "b"},
            {"source": "b", "target": "end"},
        ]

        levels = compute_dependency_levels(nodes, edges)

        self.assertEqual(levels["start"], 0)
        self.assertEqual(levels["a"], 1)
        self.assertEqual(levels["b"], 2)
        self.assertEqual(levels["end"], 3)

    def test_parallel_api_calls_same_level(self):
        """Test parallel API calls get same level"""
        nodes = [
            {"id": "start"},
            {"id": "api_1"},
            {"id": "api_2"},
            {"id": "api_3"},
            {"id": "join"},
            {"id": "end"},
        ]
        edges = [
            {"source": "start", "target": "api_1"},
            {"source": "start", "target": "api_2"},
            {"source": "start", "target": "api_3"},
            {"source": "api_1", "target": "join"},
            {"source": "api_2", "target": "join"},
            {"source": "api_3", "target": "join"},
            {"source": "join", "target": "end"},
        ]

        levels = compute_dependency_levels(nodes, edges)

        self.assertEqual(levels["start"], 0)
        self.assertEqual(levels["api_1"], 1)
        self.assertEqual(levels["api_2"], 1)
        self.assertEqual(levels["api_3"], 1)
        self.assertEqual(levels["join"], 2)
        self.assertEqual(levels["end"], 3)

    def test_diamond_dependency_pattern(self):
        """Test diamond dependency pattern"""
        nodes = [
            {"id": "start"},
            {"id": "left"},
            {"id": "right"},
            {"id": "join"},
        ]
        edges = [
            {"source": "start", "target": "left"},
            {"source": "start", "target": "right"},
            {"source": "left", "target": "join"},
            {"source": "right", "target": "join"},
        ]

        levels = compute_dependency_levels(nodes, edges)

        self.assertEqual(levels["start"], 0)
        self.assertEqual(levels["left"], 1)
        self.assertEqual(levels["right"], 1)
        self.assertEqual(levels["join"], 2)

    def test_unequal_path_lengths(self):
        """Test that longest path determines level"""
        nodes = [
            {"id": "start"},
            {"id": "fast"},
            {"id": "slow1"},
            {"id": "slow2"},
            {"id": "join"},
        ]
        edges = [
            {"source": "start", "target": "fast"},
            {"source": "start", "target": "slow1"},
            {"source": "slow1", "target": "slow2"},
            {"source": "fast", "target": "join"},
            {"source": "slow2", "target": "join"},
        ]

        levels = compute_dependency_levels(nodes, edges)

        self.assertEqual(levels["start"], 0)
        self.assertEqual(levels["fast"], 1)
        self.assertEqual(levels["slow1"], 1)
        self.assertEqual(levels["slow2"], 2)
        # Join must wait for slowest path (level 3)
        self.assertEqual(levels["join"], 3)

    def test_complex_workflow_levels(self):
        """Test complex workflow with multiple dependencies"""
        nodes = [
            {"id": "start"},
            {"id": "fetch_user"},
            {"id": "fetch_orders"},
            {"id": "fetch_inventory"},
            {"id": "validate"},
            {"id": "process"},
            {"id": "notify"},
            {"id": "end"},
        ]
        edges = [
            {"source": "start", "target": "fetch_user"},
            {"source": "start", "target": "fetch_orders"},
            {"source": "start", "target": "fetch_inventory"},
            {"source": "fetch_user", "target": "validate"},
            {"source": "fetch_orders", "target": "validate"},
            {"source": "fetch_inventory", "target": "process"},
            {"source": "validate", "target": "process"},
            {"source": "process", "target": "notify"},
            {"source": "notify", "target": "end"},
        ]

        levels = compute_dependency_levels(nodes, edges)

        self.assertEqual(levels["start"], 0)
        self.assertEqual(levels["fetch_user"], 1)
        self.assertEqual(levels["fetch_orders"], 1)
        self.assertEqual(levels["fetch_inventory"], 1)
        self.assertEqual(levels["validate"], 2)
        self.assertEqual(levels["process"], 3)
        self.assertEqual(levels["notify"], 4)
        self.assertEqual(levels["end"], 5)

    def test_cycle_detection_in_dependency_levels(self):
        """Test cycle detection in dependency levels"""
        nodes = [
            {"id": "a"},
            {"id": "b"},
            {"id": "c"},
        ]
        edges = [
            {"source": "a", "target": "b"},
            {"source": "b", "target": "c"},
            {"source": "c", "target": "a"},
        ]

        with self.assertRaises(ValueError) as cm:
            compute_dependency_levels(nodes, edges)

        self.assertIn("Cycle detected", str(cm.exception))

    def test_single_node_dependency_level(self):
        """Test single node has level 0"""
        nodes = [{"id": "single"}]
        edges = []

        levels = compute_dependency_levels(nodes, edges)
        self.assertEqual(levels["single"], 0)


class TestGroupNodesByLevel(unittest.TestCase):
    """Test cases for group_nodes_by_level function"""

    def test_simple_grouping(self):
        """Test simple node grouping by level"""
        nodes = [
            {"id": "start", "type": "start"},
            {"id": "a", "type": "action"},
            {"id": "b", "type": "action"},
            {"id": "end", "type": "end"},
        ]
        levels = {
            "start": 0,
            "a": 1,
            "b": 2,
            "end": 3,
        }

        grouped = group_nodes_by_level(nodes, levels)

        self.assertEqual(len(grouped), 4)
        self.assertEqual(grouped[0], [{"id": "start", "type": "start"}])
        self.assertEqual(grouped[1], [{"id": "a", "type": "action"}])
        self.assertEqual(grouped[2], [{"id": "b", "type": "action"}])
        self.assertEqual(grouped[3], [{"id": "end", "type": "end"}])

    def test_parallel_nodes_grouping(self):
        """Test grouping parallel nodes together"""
        nodes = [
            {"id": "start"},
            {"id": "api1"},
            {"id": "api2"},
            {"id": "api3"},
            {"id": "end"},
        ]
        levels = {
            "start": 0,
            "api1": 1,
            "api2": 1,
            "api3": 1,
            "end": 2,
        }

        grouped = group_nodes_by_level(nodes, levels)

        self.assertEqual(len(grouped), 3)
        self.assertEqual(len(grouped[0]), 1)
        self.assertEqual(len(grouped[1]), 3)
        self.assertEqual(len(grouped[2]), 1)

        # Check that all parallel APIs are in level 1
        level_1_ids = [n["id"] for n in grouped[1]]
        self.assertCountEqual(level_1_ids, ["api1", "api2", "api3"])

    def test_empty_levels(self):
        """Test empty levels"""
        nodes = []
        levels = {}

        grouped = group_nodes_by_level(nodes, levels)
        self.assertEqual(grouped, [])

    def test_single_level(self):
        """Test single level grouping"""
        nodes = [
            {"id": "a"},
            {"id": "b"},
            {"id": "c"},
        ]
        levels = {
            "a": 0,
            "b": 0,
            "c": 0,
        }

        grouped = group_nodes_by_level(nodes, levels)

        self.assertEqual(len(grouped), 1)
        self.assertEqual(len(grouped[0]), 3)

    def test_sparse_levels(self):
        """Test grouping with all levels from 0 to max"""
        nodes = [
            {"id": "a"},
            {"id": "b"},
            {"id": "c"},
        ]
        levels = {
            "a": 0,
            "b": 2,
            "c": 4,
        }

        grouped = group_nodes_by_level(nodes, levels)

        # Should have 5 levels (0, 1, 2, 3, 4)
        self.assertEqual(len(grouped), 5)
        self.assertEqual(len(grouped[0]), 1)
        self.assertEqual(len(grouped[1]), 0)  # Empty level
        self.assertEqual(len(grouped[2]), 1)
        self.assertEqual(len(grouped[3]), 0)  # Empty level
        self.assertEqual(len(grouped[4]), 1)


class TestIntegration(unittest.TestCase):
    """Integration tests combining multiple functions"""

    def test_full_workflow_pipeline(self):
        """Test complete workflow analysis pipeline"""
        nodes = [
            {"id": "start", "type": "start"},
            {"id": "fetch_user", "type": "http"},
            {"id": "fetch_orders", "type": "http"},
            {"id": "process", "type": "action"},
            {"id": "end", "type": "end"},
        ]
        edges = [
            {"source": "start", "target": "fetch_user"},
            {"source": "start", "target": "fetch_orders"},
            {"source": "fetch_user", "target": "process"},
            {"source": "fetch_orders", "target": "process"},
            {"source": "process", "target": "end"},
        ]

        # 1. Get topological order
        topo_order = topological_sort(nodes, edges)
        self.assertEqual(len(topo_order), 5)
        self.assertEqual(topo_order[0], "start")
        self.assertEqual(topo_order[-1], "end")

        # 2. Get parallel levels
        parallel_levels = get_parallel_levels(nodes, edges)
        self.assertEqual(len(parallel_levels), 4)
        self.assertCountEqual(parallel_levels[1], ["fetch_user", "fetch_orders"])

        # 3. Compute dependency levels
        dep_levels = compute_dependency_levels(nodes, edges)
        self.assertEqual(dep_levels["start"], 0)
        self.assertEqual(dep_levels["fetch_user"], 1)
        self.assertEqual(dep_levels["fetch_orders"], 1)
        self.assertEqual(dep_levels["process"], 2)
        self.assertEqual(dep_levels["end"], 3)

        # 4. Group nodes by level
        grouped = group_nodes_by_level(nodes, dep_levels)
        self.assertEqual(len(grouped), 4)
        self.assertEqual(len(grouped[1]), 2)  # Two parallel HTTP calls


if __name__ == "__main__":
    # Run with verbose output
    unittest.main(verbosity=2)

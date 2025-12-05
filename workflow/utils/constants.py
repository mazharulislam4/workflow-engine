VALID_NODE_TYPES = [
    "start",
    "end",
    "trigger",
    "action",
    "condition",
    "loop",
    "fork",
    "parallel",
    "join",
    "subworkflow",
    "http_request",
    "data_transform",
    "formation",
]

REQUIRED_WORKFLOW_FIELDS = {"nodes", "edges", "id", "name"}
REQUIRED_NODE_FIELDS = {"id", "type", "name"}
REQUIRED_EDGE_FIELDS = {"source", "target"}

from workflow.utils.safe_eval import safe_eval


def evaluate_condition(condition: str) -> bool:
    """
    Evaluate the path condition.

    Args:
        condition (str): The condition to evaluate.
    Returns:
        bool: True if the condition is met, False otherwise.
    """
    if isinstance(condition, bool):
        return condition
    if condition.lower() in ["true", "1", "yes"]:
        return True
    if condition.lower() in ["false", "0", "no"]:
        return False
    try:
        result = safe_eval(condition)
        return result
    except Exception as e:

        raise ValueError(f"Invalid path condition expression: {condition}") from e

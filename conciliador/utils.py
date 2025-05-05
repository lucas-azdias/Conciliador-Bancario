from collections import deque
from datetime import datetime
from pathlib import Path
from re import fullmatch
from typing import Any

from . import log_statements_filename


def n_flatten(nested_list: list, n: int) -> list:
    """
    Iteratively flattens a nested list to a maximum depth of `n`, ensuring the result is at most `n` levels deep.

    Parameters:
        nested_list (list): The list to be flattened.
        n (int): Maximum depth to flatten (n=1 → 1D, n=2 → 2D, etc.).

    Returns:
        list: A new list flattened to depth `n`.
    """
    if n < 1:
        raise ValueError("Depth must be at least 1")
    
    depth = 0
    temp = nested_list
    
    # Determine the current depth of the list
    while isinstance(temp, list) and temp:
        temp = temp[0]
        depth += 1
    
    # If n is greater than or equal to the current depth, return original list
    if n >= depth:
        return nested_list
    
    # Iteratively flatten until desired depth is reached
    for _ in range(depth - n):
        nested_list = [item for sublist in nested_list for item in (sublist if isinstance(sublist, list) else [sublist])]
    
    return nested_list


def get_nested_dict_element(data: dict, patterns_path: list[str]) -> Any | None:
    """
    Traverse a nested dictionary iteratively and return the first deeply nested non-dict element
    where each level matches the corresponding regex in the path.

    Parameters:
        data (dict): The dictionary to traverse.
        patterns_path (list[str]): A list of regex patterns, one for each depth level.

    Returns:
        (Any | None): The first matching value from the most deeply nested non-dict elements, or None if not found.
    """
    if not isinstance(data, dict):
        return None
    
    if not patterns_path:
        return data

    queue = deque([(data, 0)]) # Queue stores (current_dict, current_depth)

    while queue:
        current, depth = queue.popleft()

        if depth >= len(patterns_path):
            continue # Skip if we've exceeded the pattern depth

        pattern = patterns_path[depth]

        for key, value in current.items():
            if fullmatch(pattern, key): # Match key at the current depth
                if isinstance(value, dict) and depth < len(patterns_path) - 1:
                    queue.append((value, depth + 1)) # Add deeper level to the queue
                else:
                    return value # Return the first non-dict value found

    return None # Return None if no match is found


def get_nested_dict_elements(data: dict, patterns_path: list[str]) -> list[Any]:
    """
    Traverse a nested dictionary iteratively and return a list of the most deeply nested non-dict elements where each level matches the corresponding regex in the path.

    Parameters:
        data (dict): The dictionary to traverse.
        patterns_path (list[str]): A list of regex patterns, one for each depth level.

    Returns:
        list[Any]: A list of matching values from the most deeply nested non-dict elements.
    """
    if not isinstance(data, dict):
        return []
    
    if not patterns_path:
        patterns_path = [".*"]

    results = []
    queue = deque([(data, 0)])  # Queue stores (current_dict, current_depth)

    while queue:
        current, depth = queue.popleft()

        if depth >= len(patterns_path):
            continue  # Skip if we've exceeded the pattern depth

        pattern = patterns_path[depth]

        for key, value in current.items():
            if fullmatch(pattern, key):  # Match key at the current depth
                if isinstance(value, dict):
                    queue.append((value, depth + 1))  # Add deeper level to the queue
                else:
                    results.append(value)  # Collect non-dict values

    return results


def log_change(year: int, month: int, message: str, logs_folder: Path) -> None:
    """Logs changes to a log file for the given year and month.

    Args:
        year (int): Database change year.
        month (int): Database change month.
        message (str): Change message.
        logs_folder (Path): Directory where log files will be saved.
    """
    logs_folder.mkdir(parents=True, exist_ok=True)
    log_filepath = logs_folder / log_statements_filename(year, month)
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with log_filepath.open("a", encoding="utf-8") as log_file:
        log_file.write(f"{timestamp} {message}\n")
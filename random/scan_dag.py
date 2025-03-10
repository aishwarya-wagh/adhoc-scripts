#!/usr/bin/env python3

import os
import sys
import ast

def reconstruct_string(node):
    """
    Recursively reconstructs a string expression from an AST node.
    Handles plain string literals, constants, and simple 'str1 + str2' concatenations.
    Returns '<dynamic>' if it encounters something non-trivial.
    """
    if isinstance(node, ast.Str):
        # For Python <3.8, string literals are ast.Str
        return node.s
    elif isinstance(node, ast.Constant) and isinstance(node.value, str):
        # For Python 3.8+, string literals are ast.Constant
        return node.value
    elif isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = reconstruct_string(node.left)
        right = reconstruct_string(node.right)
        # If either side is <dynamic>, concatenation is effectively dynamic
        if left == "<dynamic>" or right == "<dynamic>":
            return "<dynamic>"
        return left + right
    else:
        return "<dynamic>"

def parse_file(filepath):
    """
    Parses a single .py file with the AST module.
    1) Finds all variables assigned to DAG(...) calls -> {varname: dag_name}
    2) Finds all KubernetesPodOperator(...) calls with dag=<varname> and extracts
       the 4th item in arguments (if it starts with 'etl/').
    Returns a list of (dag_name, file_path_string) tuples.
    """
    with open(filepath, "r", encoding="utf-8") as f:
        source = f.read()
    try:
        tree = ast.parse(source, mode="exec")
    except SyntaxError:
        # If the file has syntax errors or is not valid Python, skip
        return []

    # Map from variable name -> DAG name
    dag_var_to_name = {}

    # 1) Find all DAG(...) assignments:   dag = DAG("dag_name", ...)
    for node in ast.walk(tree):
        # Looking for:  dag = DAG("something", ...)
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
            call_node = node.value
            if (
                isinstance(call_node.func, ast.Name)
                and call_node.func.id == "DAG"
                and call_node.args
            ):
                # The first positional arg is the DAG name (assuming a string literal)
                first_arg = call_node.args[0]
                dag_name = reconstruct_string(first_arg)
                if dag_name != "<dynamic>":  # means we got a literal or simple concat
                    # The left side of the assignment might have multiple targets
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            dag_var_to_name[target.id] = dag_name

    results = []
    # 2) Find all KubernetesPodOperator(...) calls
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            call_node = node
            # e.g. task = KubernetesPodOperator(...)
            if isinstance(call_node.func, ast.Name) and call_node.func.id == "KubernetesPodOperator":
                # Check if there's dag=<some_var> and arguments=[...]
                dag_var = None
                arguments_list = None
                for kw in call_node.keywords:
                    # e.g. KubernetesPodOperator(dag=dag, arguments=[...])
                    if kw.arg == "dag" and isinstance(kw.value, ast.Name):
                        dag_var = kw.value.id
                    elif kw.arg == "arguments" and isinstance(kw.value, ast.List):
                        arguments_list = kw.value.elts  # list of AST nodes

                # If we found a dag variable that maps to a known DAG name:
                if dag_var and dag_var in dag_var_to_name:
                    dag_name = dag_var_to_name[dag_var]

                    # If there's at least 4 items in arguments
                    if arguments_list and len(arguments_list) >= 4:
                        # The 4th item is arguments_list[3]
                        file_path_node = arguments_list[3]
                        file_path_str = reconstruct_string(file_path_node)

                        # We only care if it starts with 'etl/'
                        if file_path_str.startswith("etl/"):
                            results.append((dag_name, file_path_str))

    return results

def scan_directory(dir_path):
    """
    Recursively scan dir_path for all .py files, parse each, and collect results.
    Returns a list of (filename, dag_name, etl_path).
    """
    all_results = []
    for root, _, files in os.walk(dir_path):
        for file in files:
            if file.endswith(".py"):
                full_path = os.path.join(root, file)
                dag_mappings = parse_file(full_path)
                for (dag_name, etl_path) in dag_mappings:
                    all_results.append((full_path, dag_name, etl_path))
    return all_results

def main():
    if len(sys.argv) < 2:
        print("Usage: python scan_dags.py /path/to/your/repo")
        sys.exit(1)

    repo_path = sys.argv[1]
    results = scan_directory(repo_path)

    # Print in a nice table or CSV-like format
    # (filename, dag_name, file_path)
    for (filename, dag_name, etl_path) in results:
        print(f"{filename}\t{dag_name}\t{etl_path}")

if __name__ == "__main__":
    main()

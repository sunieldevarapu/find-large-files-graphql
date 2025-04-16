import os
import sys
import datetime
import logging
import argparse
import csv
import time
import requests
from github import Github

GRAPHQL_URL = "https://api.github.com/graphql"

# ------------------------ Logging ------------------------
def setup_logging(output_dir=".", log_filename="large_file_scan_log.log"):
    log_filepath = os.path.join(output_dir, log_filename)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler = logging.FileHandler(log_filepath, mode='a')  # Open in append mode
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return log_filepath

def log_and_print(message, log_level='info'):
    RED = '\\033[31m'
    GREEN = '\\033[32m'
    RESET = '\\033[0m'
    log_datetime = datetime.datetime.now().strftime('%d%b%Y_%H%M%S')
    if log_level == 'error':
        logging.error(f": {message}")
        print(f"{RED}{log_datetime}: {message} {RESET}")
    elif log_level == 'success':
        logging.info(f": {message}")
        print(f"{GREEN}{log_datetime}: {message} {RESET}")
    else:
        logging.info(f": {message}")
        print(f"{log_datetime}: {message}")

def load_repositories_from_file(repo_file_path):
    repos = []
    try:
        with open(repo_file_path, "r", encoding='utf-8-sig') as file:
            for line in file:
                source_repo, target_repo = line.strip().split(INPUT_FILE_DELIMITER)
                repos.append((source_repo, target_repo))
    except Exception as e:
        log_and_print(f"...Error reading the file {repo_file_path}: {e}", "error")
    return repos

def graphql_query(query, variables, token, verify_cert=True):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    verify = verify_cert if isinstance(verify_cert, (str, bool)) else True
    response = requests.post(GRAPHQL_URL, headers=headers, json={"query": query, "variables": variables}, verify=verify)
    if response.status_code != 200:
        raise Exception(f"GraphQL error {response.status_code}: {response.text}")
    return response.json()

def get_tree_entries(owner, repo, expression, token, verify_cert=True):
    query = """
    query($owner: String!, $repo: String!, $expression: String!) {
        repository(owner: $owner, name: $repo) {
            object(expression: $expression) {
                ... on Tree {
                    entries {
                        name
                        type
                        path
                        object {
                            ... on Blob {
                                byteSize
                            }
                        }
                    }
                }
            }
        }
    }
    """
    variables = {"owner": owner, "repo": repo, "expression": expression}
    data = graphql_query(query, variables, token, verify_cert)
    return data.get("data", {}).get("repository", {}).get("object", {}).get("entries", [])

def walk_tree_recursive(owner, repo, branch, token, verify_cert=True, path_prefix="", threshold_kb=1000):
    expression = f"{branch}:{path_prefix}" if path_prefix else f"{branch}:"
    entries = get_tree_entries(owner, repo, expression, token, verify_cert)
    large_files = []
    for entry in entries:
        entry_type = entry["type"]
        entry_path = entry["path"]
        if entry_type == "blob":
            blob = entry.get("object")
            if blob:
                size_bytes = blob.get("byteSize", 0)
                size_kb = size_bytes / 1024
                size_mb = size_kb / 1024
                if size_kb > threshold_kb:
                    large_files.append({
                        "Repository": f"{owner}/{repo}",
                        "Path": entry_path,
                        "Size_KB": round(size_kb, 2),
                        "Size_MB": round(size_mb, 2),
                        "Timestamp": datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S")
                    })
        elif entry_type == "tree":
            time.sleep(0.2)
            large_files.extend(walk_tree_recursive(owner, repo, branch, token, verify_cert, entry_path, threshold_kb))
    return large_files

def scan_repo_for_large_files(repo, threshold_kb=1000):
    large_files = []
    try:
        contents = repo.get_contents("")
        while contents:
            file_content = contents.pop(0)
            if file_content.type == "dir":
                contents.extend(repo.get_contents(file_content.path))
            else:
                size_kb = file_content.size / 1024
                size_mb = size_kb / 1024
                if size_kb > threshold_kb:
                    large_files.append({
                        "Repository": repo.full_name,
                        "Path": file_content.path,
                        "Size_KB": round(size_kb, 2),
                        "Size_MB": round(size_mb, 2),
                        "Timestamp": datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S")
                    })
    except Exception as e:
        log_and_print(f"Error scanning {repo.full_name}: {e}", 'error')
    return large_files

def parse_arguments():
    parser = argparse.ArgumentParser(description="Scan GitHub repos for large files using GraphQL API")
    parser.add_argument("--token", required=True, help="GitHub personal access token")
    parser.add_argument("--csv-input", required=True, help="Path to input CSV file (with 'repository' column)")
    parser.add_argument("--csv-output", required=True, help="Path to output CSV report")
    parser.add_argument("--size-threshold-kb", type=int, default=1000, help="Minimum file size in KB to report")
    parser.add_argument("--cert", help="Path to SSL certificate or CA bundle, or use 'False' to disable verification")
    return parser.parse_args()

def main():
    args = parse_arguments()
    token = args.token
    csv_input = args.csv_input
    csv_output = args.csv_output
    threshold_kb = args.size_threshold_kb
    verify_cert = True
    if args.cert:
        if args.cert.lower() == 'false':
            verify_cert = False
        else:
            verify_cert = args.cert  # Path to PEM file

    # Setup logging to a single file without deleting previous data
    setup_logging()

    g = Github(token)
    all_large_files = []

    try:
        with open(csv_input, newline='', encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                full_repo = row.get("repository", "").strip()
                if not full_repo or "/" not in full_repo:
                    log_and_print(f"Skipping invalid repo: {full_repo}", 'warning')
                    continue
                try:
                    repo = g.get_repo(full_repo)
                    log_and_print(f"Scanning {repo.full_name}...")
                    repo_large_files = scan_repo_for_large_files(repo, threshold_kb=threshold_kb)
                    all_large_files.extend(repo_large_files)
                    log_and_print(f"{len(repo_large_files)} large files found in {repo.full_name}")
                except Exception as e:
                    log_and_print(f"Error accessing {full_repo}: {e}", 'error')
    except Exception as e:
        log_and_print(f"Error reading CSV input file: {e}", 'error')
        sys.exit(1)

    if all_large_files:
        try:
            # Open the existing CSV file in append mode
            with open(csv_output, mode="a", newline="", encoding="utf-8") as f:
                fieldnames = list(all_large_files[0].keys())
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                # Write rows to the existing file
                writer.writerows(all_large_files)
            log_and_print(f"Report appended to {csv_output} with {len(all_large_files)} large files.", 'success')
        except Exception as e:
            log_and_print(f"Error writing output CSV: {e}", 'error')
    else:
        log_and_print("No large files found above threshold.", 'success')

if __name__ == "__main__":
    main()

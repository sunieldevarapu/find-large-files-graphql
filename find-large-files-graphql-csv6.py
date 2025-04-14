import os
import sys
import datetime
import logging
import argparse
import csv
import time
import requests  # ✅ REQUIRED for all GitHub API/GraphQL POSTs


import time

GRAPHQL_URL = "https://api.github.com/graphql"

# ------------------------ Logging ------------------------

def setup_logging(output_dir=".", script_name="large_file_scanner"):
    timestamp = datetime.datetime.now().strftime("%d%b%Y_%H%M")
    log_filename = os.path.join(output_dir, f"{script_name}_LOG_{timestamp}.log")

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler(log_filename)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return log_filename

def log_and_print(message, level='info'):
    if level == 'error':
        logging.error(message)
    elif level == 'warning':
        logging.warning(message)
    elif level == 'success':
        logging.info(f"SUCCESS: {message}")
    else:
        logging.info(message)

# ------------------------ GitHub GraphQL ------------------------

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
                        "Size_MB": round(size_mb, 2)
                    })
        elif entry_type == "tree":
            time.sleep(0.2)
            large_files.extend(walk_tree_recursive(owner, repo, branch, token, verify_cert, entry_path, threshold_kb))
    return large_files

# ------------------------ Main ------------------------

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

    setup_logging()

    all_large_files = []

    try:
        with open(csv_input, newline='', encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                full_repo = row.get("repository", "").strip()
                if not full_repo or "/" not in full_repo:
                    log_and_print(f"Skipping invalid repo: {full_repo}", 'warning')
                    continue

                owner, repo = full_repo.split("/")
                log_and_print(f"Scanning {owner}/{repo}...")

                branch_query = """
                query($owner: String!, $repo: String!) {
                  repository(owner: $owner, name: $repo) {
                    defaultBranchRef {
                      name
                    }
                  }
                }
                """
                try:
                    result = graphql_query(branch_query, {"owner": owner, "repo": repo}, token, verify_cert)
                    default_branch = result.get("data", {}).get("repository", {}).get("defaultBranchRef", {}).get("name")
                    if not default_branch:
                        log_and_print(f"Skipping {owner}/{repo}: No default branch.", 'warning')
                        continue
                except Exception as e:
                    log_and_print(f"Error fetching default branch for {owner}/{repo}: {e}", 'error')
                    continue

                try:
                    repo_large_files = walk_tree_recursive(owner, repo, default_branch, token, verify_cert, threshold_kb=threshold_kb)
                    all_large_files.extend(repo_large_files)
                    log_and_print(f"{len(repo_large_files)} large files found in {owner}/{repo}")
                except Exception as e:
                    log_and_print(f"Error scanning {owner}/{repo}: {e}", 'error')
    except Exception as e:
        log_and_print(f"Error reading CSV input file: {e}", 'error')
        sys.exit(1)

    if all_large_files:
        try:
            with open(csv_output, mode="w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=all_large_files[0].keys())
                writer.writeheader()
                writer.writerows(all_large_files)
            log_and_print(f"Report written to {csv_output} with {len(all_large_files)} large files.", 'success')
        except Exception as e:
            log_and_print(f"Error writing output CSV: {e}", 'error')
    else:
        log_and_print("No large files found above threshold.", 'success')

if __name__ == "__main__":
    main()

import os
import csv
import requests # type: ignore
import argparse
import time

# --- CONFIG ---
REPO_OWNER = "sunieldevarapu"      # e.g., "octocat"
REPO_NAME = "find-large-files-graphql"        # e.g., "Hello-World"
SIZE_THRESHOLD_KB = 50   # File size threshold
OUTPUT_FILE = "large_files_report.txt"
# Use GitHub token from environment
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
if not GITHUB_TOKEN:
    raise Exception("GITHUB_TOKEN environment variable not set.")

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Content-Type": "application/json"
}

GRAPHQL_URL = "https://api.github.com/graphql"

def parse_arguments():
    parser = argparse.ArgumentParser(description="Scan GitHub repos for large files using GraphQL API")
    parser.add_argument("--csv-input", required=True, help="Path to CSV file with repositories")
    parser.add_argument("--csv-output", required=True, help="Path to output CSV report")
    parser.add_argument("--size-threshold-kb", type=int, default=1000, help="File size threshold in KB")
    return parser.parse_args()

def graphql_query(query, variables, token):
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(GRAPHQL_URL, headers=headers, json={"query": query, "variables": variables})
    if response.status_code != 200:
        raise Exception(f"GraphQL error {response.status_code}: {response.text}")
    return response.json()

def get_tree_entries(owner, repo, expression, token):
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
    variables = {
        "owner": owner,
        "repo": repo,
        "expression": expression  # e.g., "main:" or "main:src/utils"
    }
    data = graphql_query(query, variables, token)
    return data.get("data", {}).get("repository", {}).get("object", {}).get("entries", [])

def walk_tree_recursive(owner, repo, branch, token, path_prefix="", threshold_kb=1000):
    expression = f"{branch}:{path_prefix}" if path_prefix else f"{branch}:"
    entries = get_tree_entries(owner, repo, expression, token)

    large_files = []

    for entry in entries:
        entry_type = entry["type"]
        entry_path = entry["path"]

        if entry_type == "blob":
            blob = entry.get("object")
            if blob and blob.get("byteSize", 0) > threshold_kb * 1024:
                size_kb = blob["byteSize"] / 1024
                large_files.append({
                    "Repository": f"{owner}/{repo}",
                    "Path": entry_path,
                    "Size_KB": round(size_kb, 2)
                })
        elif entry_type == "tree":
            # Recursive call for subfolders
            sub_path = entry_path
            time.sleep(0.2)  # Be nice to GitHub API
            large_files.extend(walk_tree_recursive(owner, repo, branch, token, sub_path, threshold_kb))

    return large_files

def main():
    args = parse_arguments()

    github_token = os.getenv("GITHUB_TOKEN")
    if not github_token:
        raise Exception("GITHUB_TOKEN not found in environment variables.")

    all_large_files = []

    with open(args.csv_input, newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            full_repo = row["repository"].strip()
            if not full_repo or "/" not in full_repo:
                print(f"Skipping invalid repo entry: {full_repo}")
                continue

            owner, repo = full_repo.split("/")
            print(f"Scanning {owner}/{repo}...")

            # Get default branch name via GraphQL
            branch_query = """
            query($owner: String!, $repo: String!) {
              repository(owner: $owner, name: $repo) {
                defaultBranchRef {
                  name
                }
              }
            }
            """
            default_branch_ref = result.get("data", {}).get("repository", {}).get("defaultBranchRef")
            if not default_branch_ref:
                print(f"Repository {owner}/{repo} has no default branch or is inaccessible.")
                continue  # skip this repo
            
            default_branch = default_branch_ref["name"]

            variables = {"owner": owner, "repo": repo}
            #result = graphql_query(branch_query, variables, github_token)
            #default_branch = result["data"]["repository"]["defaultBranchRef"]["name"]
            default_branch = None
            try:
                result = graphql_query(branch_query, variables, github_token)
                default_branch_ref = result.get("data", {}).get("repository", {}).get("defaultBranchRef")
                if default_branch_ref:
                    default_branch = default_branch_ref["name"]
                else:
                    print(f"Skipping {owner}/{repo} — no default branch found or repo is inaccessible.")
                    continue
            except Exception as e:
                print(f"Error fetching default branch for {owner}/{repo}: {e}")
                continue



            try:
                repo_large_files = walk_tree_recursive(owner, repo, default_branch, github_token, threshold_kb=args.size_threshold_kb)
                all_large_files.extend(repo_large_files)
            except Exception as e:
                print(f"Error scanning {full_repo}: {e}")

    if all_large_files:
        with open(args.csv_output, mode="w", newline='', encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_large_files[0].keys())
            writer.writeheader()
            writer.writerows(all_large_files)
        print(f"\n Report saved to '{args.csv_output}' with {len(all_large_files)} large files.\n")
    else:
        print("\n No large files found in any repository.\n")

if __name__ == "__main__":
    main()

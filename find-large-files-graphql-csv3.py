import os
import csv
import requests # type: ignore
import time

GRAPHQL_URL = "https://api.github.com/graphql"

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
    variables = {"owner": owner, "repo": repo, "expression": expression}
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
            large_files.extend(walk_tree_recursive(owner, repo, branch, token, entry_path, threshold_kb))
    return large_files

def main():
    token = os.environ.get("GITHUB_TOKEN")
    csv_input = os.environ.get("CSV_INPUT", "repos.csv")
    csv_output = os.environ.get("CSV_OUTPUT", "large_files_report.csv")
    threshold_kb = int(os.environ.get("SIZE_THRESHOLD_KB", 1000))

    if not token:
        raise Exception("GITHUB_TOKEN is not set.")

    all_large_files = []

    with open(csv_input, newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            full_repo = row["repository"].strip()
            if not full_repo or "/" not in full_repo:
                print(f"Skipping invalid repo: {full_repo}")
                continue

            owner, repo = full_repo.split("/")

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
                result = graphql_query(branch_query, {"owner": owner, "repo": repo}, token)
                default_branch = result.get("data", {}).get("repository", {}).get("defaultBranchRef", {}).get("name")
                if not default_branch:
                    print(f"Skipping {owner}/{repo}: No default branch.")
                    continue
            except Exception as e:
                print(f"Error fetching branch for {owner}/{repo}: {e}")
                continue

            try:
                repo_large_files = walk_tree_recursive(owner, repo, default_branch, token, threshold_kb=threshold_kb)
                all_large_files.extend(repo_large_files)
            except Exception as e:
                print(f"Error scanning {owner}/{repo}: {e}")

    if all_large_files:
        with open(csv_output, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_large_files[0].keys())
            writer.writeheader()
            writer.writerows(all_large_files)
        print(f"Report written to {csv_output}")
    else:
        print("No large files found.")

if __name__ == "__main__":
    main()

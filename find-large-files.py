import os
import requests # type: ignore
import time

# --- CONFIG ---
REPO_OWNER = "sunieldevarapu"      # e.g., "octocat"
REPO_NAME = "find-large-files-graphql"        # e.g., "Hello-World"
SIZE_THRESHOLD_KB = 10   # File size threshold
OUTPUT_FILE = "large_files_report.txt"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Content-Type": "application/json"
}
# -------------


def run_query(query, variables=None, retries=3):
    url = "https://api.github.com/graphql"
    for attempt in range(retries):
        response = requests.post(url, json={"query": query, "variables": variables}, headers=HEADERS)
        if response.status_code == 200:
            return response.json()
        time.sleep(1)
    raise Exception(f"Query failed after {retries} retries: {response.status_code} {response.text}")


def get_default_branch_sha():
    query = """
    query($owner: String!, $name: String!) {
      repository(owner: $owner, name: $name) {
        defaultBranchRef {
          target {
            ... on Commit {
              oid
            }
          }
        }
      }
    }
    """
    result = run_query(query, {"owner": REPO_OWNER, "name": REPO_NAME})
    return result["data"]["repository"]["defaultBranchRef"]["target"]["oid"]


def get_tree_entries(expression):
    query = """
    query($owner: String!, $name: String!, $expression: String!) {
      repository(owner: $owner, name: $name) {
        object(expression: $expression) {
          ... on Tree {
            entries {
              name
              path
              type
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
        "owner": REPO_OWNER,
        "name": REPO_NAME,
        "expression": expression
    }
    result = run_query(query, variables)
    tree = result["data"]["repository"]["object"]
    return tree["entries"] if tree else []


def walk_tree_recursively(commit_sha, prefix=""):
    large_files = []
    stack = [prefix]

    while stack:
        current_path = stack.pop()
        expr = f"{commit_sha}:{current_path}" if current_path else f"{commit_sha}"
        entries = get_tree_entries(expr)

        for entry in entries:
            entry_path = entry["path"]
            entry_type = entry["type"]
            size_info = entry.get("object")

            if entry_type == "blob" and size_info:
                size_kb = size_info["byteSize"] / 1024
                if size_kb > SIZE_THRESHOLD_KB:
                    large_files.append((entry_path, round(size_kb, 2)))
            elif entry_type == "tree":
                stack.append(entry_path)

    return large_files


def save_results_to_file(files):
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        if not files:
            f.write("No large files found.\n")
        else:
            f.write(f"Files larger than {SIZE_THRESHOLD_KB}KB:\n\n")
            for path, size in sorted(files, key=lambda x: -x[1]):
                f.write(f"{path} - {size} KB\n")


def main():
    print(f"Scanning {REPO_OWNER}/{REPO_NAME} for large files...")
    sha = get_default_branch_sha()
    large_files = walk_tree_recursively(sha)

    if not large_files:
        print("No large files found.")
    else:
        print(f"\n Files larger than {SIZE_THRESHOLD_KB}KB:\n")
        for path, size in sorted(large_files, key=lambda x: -x[1]):
            print(f"{path} - {size} KB")

    save_results_to_file(large_files)
    print(f"\n Results saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()

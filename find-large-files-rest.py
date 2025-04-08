import os
import requests # type: ignore

REPO_OWNER = "sunieldevarapu"
REPO_NAME = "find-large-files-graphql"
SIZE_THRESHOLD_KB = 20
OUTPUT_FILE = "large_files_report.txt"

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
if not GITHUB_TOKEN:
    raise Exception("GITHUB_TOKEN environment variable not set.")

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

def get_default_branch_sha():
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp.json()["default_branch"]

def get_tree_sha(branch):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/git/refs/heads/{branch}"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp.json()["object"]["sha"]

def get_tree(tree_sha):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/git/trees/{tree_sha}?recursive=1"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp.json()["tree"]

def get_blob_size(blob_sha):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/git/blobs/{blob_sha}"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp.json()["size"]

def find_large_files():
    print(f"Scanning {REPO_OWNER}/{REPO_NAME} for files > {SIZE_THRESHOLD_KB}KB")
    branch = get_default_branch_sha()
    commit_sha = get_tree_sha(branch)
    tree = get_tree(commit_sha)

    large_files = []

    for item in tree:
        if item.get("type") == "blob":
            path = item.get("path", "<unknown path>")
            blob_sha = item["sha"]
            size_bytes = item.get("size")

            # Fallback to blob API if size is not provided
            if size_bytes is None:
                size_bytes = get_blob_size(blob_sha, path)

            size_kb = size_bytes / 1024
            print(f"{path} — {round(size_kb, 2)} KB")

            if size_kb >= SIZE_THRESHOLD_KB:
                large_files.append((path, round(size_kb, 2)))

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        if not large_files:
            f.write("No large files found.\n")
        else:
            f.write(f"Files larger than {SIZE_THRESHOLD_KB}KB:\n\n")
            for path, size in sorted(large_files, key=lambda x: -x[1]):
                f.write(f"{path} - {size} KB\n")

    print(f"Report saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    find_large_files()

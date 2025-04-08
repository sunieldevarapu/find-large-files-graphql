import os
from github import Github

REPO_OWNER = "sunieldevarapu"
REPO_NAME = "find-large-files-graphql"
SIZE_THRESHOLD_KB = 20
OUTPUT_FILE = "large_files_report.txt"

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
if not GITHUB_TOKEN:
    raise Exception("GITHUB_TOKEN environment variable not set.")

def find_large_files():
    g = Github(GITHUB_TOKEN)
    repo = g.get_repo(f"{REPO_OWNER}/{REPO_NAME}")

    print(f"Scanning repo: {REPO_OWNER}/{REPO_NAME}")

    tree = repo.get_git_tree(sha=repo.default_branch, recursive=True).tree

    large_files = []

    for item in tree:
        if item.type == "blob":
            size_kb = item.size / 1024
            print(f"{item.path} — {round(size_kb, 2)} KB")
            if size_kb >= SIZE_THRESHOLD_KB:
                large_files.append((item.path, round(size_kb, 2)))

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        if not large_files:
            f.write("No large files found.\n")
        else:
            f.write(f"Files larger than {SIZE_THRESHOLD_KB}KB:\n\n")
            for path, size in sorted(large_files, key=lambda x: -x[1]):
                f.write(f"{path} - {size} KB\n")

    print(f"Report written to: {OUTPUT_FILE}")

if __name__ == "__main__":
    find_large_files()

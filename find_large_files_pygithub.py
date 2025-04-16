import os
import sys
import datetime
import logging
import argparse
import csv
import time
from github import Github

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

def log_and_print(message, level='info'):
    if level == 'error':
        logging.error(message)
    elif level == 'warning':
        logging.warning(message)
    elif level == 'success':
        logging.info(f"SUCCESS: {message}")
    else:
        logging.info(message)

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

# ------------------------ Main ------------------------
def parse_arguments():
    parser = argparse.ArgumentParser(description="Scan GitHub repos for large files using PyGithub")
    parser.add_argument("--token", required=True, help="GitHub personal access token")
    parser.add_argument("--csv-input", required=True, help="Path to input CSV file (with 'repository' column)")
    parser.add_argument("--csv-output", required=True, help="Path to output CSV report")
    parser.add_argument("--size-threshold-kb", type=int, default=1000, help="Minimum file size in KB to report")
    return parser.parse_args()

def main():
    args = parse_arguments()
    token = args.token
    csv_input = args.csv_input
    csv_output = "output_large_files.csv"  # Changed output file name
    threshold_kb = args.size_threshold_kb

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

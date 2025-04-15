import os
import requests
import pandas as pd
from datetime import datetime
from typing import List, Dict
from dotenv import load_dotenv

class GitHubRepoScanner:
    def __init__(self, token: str, enterprise_url: str, size_limit_kb: int):
        """Initialize the scanner with GitHub credentials and size limit."""
        self.token = token
        self.enterprise_url = enterprise_url.rstrip('/')  # Remove trailing slash if present
        self.size_limit_kb = size_limit_kb
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/vnd.github.v4+json'
        }

    def read_repos_from_csv(self, input_file: str) -> List[str]:
        """Read repository names from input CSV file."""
        try:
            df = pd.read_csv(input_file)
            if 'repository' not in df.columns:
                raise ValueError("CSV file must contain a 'repository' column")
            return df['repository'].tolist()
        except Exception as e:
            print(f"Error reading input CSV: {e}")
            return []

    def execute_graphql_query(self, repo_name: str) -> Dict:
        """Execute GraphQL query for a repository."""
        query = """
        query ($owner: String!, $name: String!) {
          repository(owner: $owner, name: $name) {
            object(expression: "HEAD") {
              ... on Commit {
                tree {
                  entries {
                    name
                    type
                    object {
                      ... on Blob {
                        byteSize
                      }
                    }
                    submodules: object {
                      ... on Tree {
                        entries {
                          name
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
              }
            }
          }
        }
        """
        
        try:
            owner, name = repo_name.split('/')
        except ValueError:
            print(f"Invalid repository format: {repo_name}. Expected format: owner/repo")
            return None

        variables = {
            "owner": owner,
            "name": name
        }

        try:
            response = requests.post(
                f"{self.enterprise_url}/api/graphql",
                json={'query': query, 'variables': variables},
                headers=self.headers,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error querying repository {repo_name}: {response.status_code}")
                print(f"Response: {response.text}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Network error while querying {repo_name}: {e}")
            return None

    def process_entries(self, entries: List[Dict], repo_name: str, path: str = "") -> List[Dict]:
        """Recursively process file entries and find large files."""
        large_files = []
        
        for entry in entries:
            current_path = f"{path}/{entry['name']}" if path else entry['name']
            
            if entry['type'] == 'blob' and entry['object']:
                size_kb = entry['object']['byteSize'] / 1024
                if size_kb > self.size_limit_kb:
                    large_files.append({
                        'repository': repo_name,
                        'file_path': current_path,
                        'size_kb': round(size_kb, 2),
                        'size_mb': round(size_kb / 1024, 2)
                    })
            
            # Process submodules/directories if they exist
            if entry.get('submodules') and entry['submodules'].get('entries'):
                large_files.extend(
                    self.process_entries(
                        entry['submodules']['entries'],
                        repo_name,
                        current_path
                    )
                )
        
        return large_files

    def scan_repository(self, repo_name: str) -> List[Dict]:
        """Scan a repository for large files."""
        data = self.execute_graphql_query(repo_name)
        if not data or 'errors' in data:
            print(f"Error scanning repository {repo_name}")
            if data and 'errors' in data:
                print(f"GraphQL Errors: {data['errors']}")
            return []

        try:
            tree_data = data['data']['repository']['object']['tree']
            if not tree_data:
                print(f"No tree data found for repository {repo_name}")
                return []
                
            return self.process_entries(tree_data['entries'], repo_name)
        except Exception as e:
            print(f"Error processing repository data for {repo_name}: {e}")
            return []

    def scan_repositories(self, input_file: str, output_file: str = None):
        """Main method to scan repositories and write results."""
        if output_file is None:
            output_file = f"large_files_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        repos = self.read_repos_from_csv(input_file)
        if not repos:
            print("No repositories found in input file")
            return

        print(f"Found {len(repos)} repositories to scan")
        all_large_files = []

        for i, repo in enumerate(repos, 1):
            print(f"Scanning repository {i}/{len(repos)}: {repo}")
            large_files = self.scan_repository(repo)
            if large_files:
                print(f"Found {len(large_files)} large files in {repo}")
                all_large_files.extend(large_files)
            else:
                print(f"No large files found in {repo}")

        if all_large_files:
            df = pd.DataFrame(all_large_files)
            df = df.sort_values(['repository', 'size_kb'], ascending=[True, False])
            df.to_csv(output_file, index=False)
            print(f"\nResults written to {output_file}")
            print(f"Total large files found: {len(all_large_files)}")
        else:
            print("\nNo large files found in any repository")

def main():
    # Load environment variables from .env file
    load_dotenv()
    
    # Get configuration from environment variables
    token = os.getenv('GITHUB_TOKEN')
    enterprise_url = os.getenv('GITHUB_ENTERPRISE_URL')
    
    if not token or not enterprise_url:
        print("Error: GitHub token or enterprise URL not found in environment variables")
        print("Please ensure you have a .env file with GITHUB_TOKEN and GITHUB_ENTERPRISE_URL")
        return

    # Configuration
    size_limit_kb = 1024  # Files larger than 1MB (can be modified as needed)
    input_file = "repositories.csv"
    
    # Create scanner instance and run scan
    try:
        scanner = GitHubRepoScanner(token, enterprise_url, size_limit_kb)
        scanner.scan_repositories(input_file)
    except Exception as e:
        print(f"An error occurred during scanning: {e}")

if __name__ == "__main__":
    main()

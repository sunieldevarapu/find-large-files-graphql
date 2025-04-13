import csv
import os
import sys
import ssl
import requests
from datetime import datetime
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from typing import List, Dict

def create_ssl_context(cert_path: str, key_path: str) -> ssl.SSLContext:
    """Create SSL context with client certificates."""
    context = ssl.create_default_context()
    context.load_cert_chain(certfile=cert_path, keyfile=key_path)
    return context

def read_repos_from_csv(input_file: str) -> List[str]:
    """Read repository URLs from input CSV file."""
    repos = []
    with open(input_file, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        for row in reader:
            repos.append(row[0])
    return repos

def get_repo_info_query() -> str:
    """Return GraphQL query for repository information."""
    return """
    query($owner: String!, $name: String!) {
        repository(owner: $owner, name: $name) {
            name
            owner {
                login
            }
            stargazerCount
            forkCount
            issues(states: OPEN) {
                totalCount
            }
            updatedAt
            primaryLanguage {
                name
            }
            diskUsage
            defaultBranchRef {
                name
                target {
                    ... on Commit {
                        tree {
                            entries {
                                name
                                type
                                object {
                                    ... on Blob {
                                        byteSize
                                    }
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
                                path
                            }
                        }
                    }
                }
            }
            licenseInfo {
                name
            }
        }
    }
    """

def format_size(size_in_bytes: int) -> str:
    """Convert bytes to human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_in_bytes < 1024:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024
    return f"{size_in_bytes:.2f} GB"

def find_large_files(entries, threshold_kb=1024, path_prefix="") -> List[Dict]:
    """Recursively find large files in repository."""
    large_files = []
    for entry in entries:
        if entry['type'] == 'blob':
            size_bytes = entry.get('object', {}).get('byteSize', 0)
            if size_bytes > threshold_kb * 1024:  # Convert KB to bytes
                large_files.append({
                    'path': f"{path_prefix}/{entry['path']}".lstrip('/'),
                    'size': size_bytes
                })
        elif entry['type'] == 'tree' and 'entries' in entry.get('object', {}):
            new_prefix = f"{path_prefix}/{entry['path']}" if path_prefix else entry['path']
            large_files.extend(find_large_files(
                entry['object']['entries'],
                threshold_kb,
                new_prefix
            ))
    return large_files

def scan_repository(client: Client, repo_url: str) -> Dict:
    """Scan a single repository using GraphQL and return its information."""
    try:
        # Extract owner and repo name from URL
        _, _, _, owner, repo_name = repo_url.rstrip('/').split('/')
        
        # Execute GraphQL query
        query = gql(get_repo_info_query())
        variables = {
            'owner': owner,
            'name': repo_name
        }
        
        result = client.execute(query, variable_values=variables)
        repo = result['repository']
        
        # Find large files (>1MB by default)
        large_files = find_large_files(
            repo['defaultBranchRef']['target']['tree']['entries'],
            threshold_kb=1024  # Files larger than 1MB
        )
        
        return {
            'repository_url': repo_url,
            'name': repo['name'],
            'owner': repo['owner']['login'],
            'stars': repo['stargazerCount'],
            'forks': repo['forkCount'],
            'open_issues': repo['issues']['totalCount'],
            'last_updated': repo['updatedAt'],
            'language': repo['primaryLanguage']['name'] if repo['primaryLanguage'] else 'None',
            'size_kb': repo['diskUsage'],
            'default_branch': repo['defaultBranchRef']['name'],
            'license': repo['licenseInfo']['name'] if repo['licenseInfo'] else 'None',
            'large_files': [
                {
                    'path': f"{repo['name']}/{file['path']}",
                    'size': format_size(file['size'])
                }
                for file in large_files
            ],
            'scan_date': datetime.now().isoformat()
        }
    except Exception as e:
        return {
            'repository_url': repo_url,
            'error': str(e)
        }

def main():
    # Get certificate paths from environment variables
    cert_path = os.getenv('GITHUB_CERT_PATH')
    key_path = os.getenv('GITHUB_KEY_PATH')
    enterprise_url = os.getenv('GITHUB_ENTERPRISE_URL')
    
    if not all([cert_path, key_path, enterprise_url]):
        print("Error: Required environment variables not set")
        print("Please set GITHUB_CERT_PATH, GITHUB_KEY_PATH, and GITHUB_ENTERPRISE_URL")
        sys.exit(1)

    # Get input and output file paths
    input_file = sys.argv[1] if len(sys.argv) > 1 else 'input_repos.csv'
    output_file = sys.argv[2] if len(sys.argv) > 2 else 'output_repos.csv'

    try:
        # Create SSL context with certificates
        ssl_context = create_ssl_context(cert_path, key_path)
        
        # Set up GraphQL client with certificate authentication
        transport = RequestsHTTPTransport(
            url=f"{enterprise_url}/api/graphql",
            verify=True,
            cert=(cert_path, key_path),
            ssl_context=ssl_context
        )
        
        client = Client(
            transport=transport,
            fetch_schema_from_transport=True
        )

        # Read repositories from input file
        repos = read_repos_from_csv(input_file)
        
        # Scan repositories
        results = []
        for repo_url in repos:
            print(f"Scanning repository: {repo_url}")
            result = scan_repository(client, repo_url)
            results.append(result)

        # Write results to output CSV
        fieldnames = ['repository_url', 'name', 'owner', 'stars', 'forks', 
                     'open_issues', 'last_updated', 'language', 'size_kb',
                     'default_branch', 'license', 'large_files', 'scan_date', 'error']
        
        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)

        print(f"Scan complete. Results written to {output_file}")

    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
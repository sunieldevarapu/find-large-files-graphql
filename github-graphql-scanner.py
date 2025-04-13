import csv
import os
import sys
import argparse
import logging
import json
import time
import requests
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse
import jwt  # PyJWT package needed for certificate auth

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('github-graphql-scanner')

# GitHub GraphQL API endpoint
GITHUB_GRAPHQL_URL = "https://api.github.com/graphql"
# GitHub REST API base URL (used for tree API which is more suitable for file scanning)
GITHUB_REST_API_URL = "https://api.github.com"

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Scan GitHub repositories using GraphQL from a CSV file')
    parser.add_argument('--input', required=True, help='Input CSV file with GitHub repositories')
    parser.add_argument('--output', required=True, help='Output CSV file for results')
    parser.add_argument('--auth-type', choices=['token', 'cert'], default='cert', 
                      help='Authentication type: token or certificate')
    parser.add_argument('--token', help='GitHub personal access token')
    parser.add_argument('--app-id', help='GitHub App ID (for certificate auth)')
    parser.add_argument('--installation-id', help='GitHub App Installation ID (for certificate auth)')
    parser.add_argument('--private-key-path', help='Path to the GitHub private key PEM file')
    parser.add_argument('--rate-limit-pause', type=int, default=2, 
                      help='Seconds to pause between API calls to avoid rate limiting')
    parser.add_argument('--large-file-threshold', type=int, default=1024, 
                      help='Size threshold in KB to identify large files (default: 1024 KB = 1 MB)')
    parser.add_argument('--max-files-per-repo', type=int, default=10, 
                      help='Maximum number of large files to report per repository (default: 10)')
    return parser.parse_args()

def generate_jwt_token(app_id, private_key_path):
    """Generate a JWT to authenticate as a GitHub App."""
    logger.info(f"Generating JWT token for GitHub App ID: {app_id}")
    
    # Read the private key
    with open(private_key_path, 'r') as key_file:
        private_key = key_file.read()
    
    # Create JWT token that expires in 10 minutes
    now = int(time.time())
    payload = {
        'iat': now,                # Issued at time
        'exp': now + (10 * 60),    # JWT expiration time (10 minute maximum)
        'iss': app_id              # GitHub App's identifier
    }
    
    # Create JWT token
    token = jwt.encode(payload, private_key, algorithm='RS256')
    return token

def get_installation_token(app_id, installation_id, private_key_path):
    """Get an installation access token for a GitHub App."""
    logger.info(f"Getting installation token for installation ID: {installation_id}")
    jwt_token = generate_jwt_token(app_id, private_key_path)
    
    url = f"https://api.github.com/app/installations/{installation_id}/access_tokens"
    headers = {
        'Authorization': f'Bearer {jwt_token}',
        'Accept': 'application/vnd.github.v3+json'
    }
    
    response = requests.post(url, headers=headers)
    if response.status_code != 201:
        logger.error(f"Failed to get installation token: {response.status_code} - {response.text}")
        raise Exception(f"Failed to get installation token: {response.status_code}")
    
    return response.json()['token']

def create_auth_headers(args):
    """Create authorization headers based on the authentication type."""
    if args.auth_type == 'token':
        if not args.token:
            raise ValueError("GitHub token is required when using token authentication")
        token = args.token
        logger.info("Using provided GitHub token for authentication")
    else:  # Certificate auth
        if not all([args.app_id, args.installation_id, args.private_key_path]):
            raise ValueError("App ID, Installation ID, and private key path are required for certificate authentication")
        
        if not os.path.isfile(args.private_key_path):
            raise FileNotFoundError(f"Private key file not found: {args.private_key_path}")
        
        # Get installation token using GitHub App credentials
        token = get_installation_token(args.app_id, args.installation_id, args.private_key_path)
        logger.info("Generated installation token using GitHub App certificate")
    
    return {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
    }, token

def read_repositories(input_file):
    """Read repositories from the input CSV file."""
    repositories = []
    try:
        with open(input_file, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            # Validate header
            if 'repository_url' not in reader.fieldnames:
                logger.error("Input CSV must contain 'repository_url' column")
                return []
            
            for row in reader:
                repositories.append(row['repository_url'])
        
        logger.info(f"Read {len(repositories)} repositories from {input_file}")
        return repositories
    except Exception as e:
        logger.error(f"Error reading input file: {e}")
        return []

def normalize_repo_url(url):
    """Normalize GitHub repository URL to owner/repo format."""
    # Handle URLs like https://github.com/owner/repo
    parsed = urlparse(url)
    if parsed.netloc == 'github.com':
        path_parts = parsed.path.strip('/').split('/')
        if len(path_parts) >= 2:
            return f"{path_parts[0]}/{path_parts[1]}"
    
    # Handle owner/repo format directly
    if '/' in url and len(url.split('/')) == 2:
        return url
    
    logger.warning(f"Invalid repository format: {url}")
    return None

def run_graphql_query(headers, query, variables=None):
    """Run a GraphQL query against the GitHub API."""
    payload = {
        'query': query
    }
    
    if variables:
        payload['variables'] = variables
    
    response = requests.post(GITHUB_GRAPHQL_URL, headers=headers, json=payload)
    
    if response.status_code != 200:
        logger.error(f"GraphQL query failed: {response.status_code} - {response.text}")
        raise Exception(f"GraphQL query failed with status code {response.status_code}")
    
    result = response.json()
    if 'errors' in result:
        logger.error(f"GraphQL errors: {result['errors']}")
        # Return the result anyway, as it might contain partial data
    
    return result

def format_file_size(size_bytes):
    """Format file size in bytes to a human-readable format (KB or MB)."""
    if size_bytes < 1024 * 1024:  # Less than 1 MB
        return f"{size_bytes / 1024:.2f} KB"
    else:  # 1 MB or more
        return f"{size_bytes / (1024 * 1024):.2f} MB"

def scan_repository_for_large_files(token, owner, repo_name, default_branch, threshold_kb, max_files, rate_limit_pause):
    """Scan a repository for large files using the Git Tree API."""
    large_files = []
    
    # Use REST API to get the repository tree recursively
    rest_headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github.v3+json'
    }
    
    # Get the tree for the default branch
    tree_url = f"{GITHUB_REST_API_URL}/repos/{owner}/{repo_name}/git/trees/{default_branch}?recursive=1"
    
    try:
        logger.info(f"Fetching file tree for {owner}/{repo_name}")
        response = requests.get(tree_url, headers=rest_headers)
        
        if response.status_code != 200:
            logger.error(f"Failed to get repository tree: {response.status_code} - {response.text}")
            return []
        
        tree_data = response.json()
        
        # Check if the response is truncated (too large)
        if tree_data.get('truncated', False):
            logger.warning(f"Repository tree is truncated due to size limits. Some files may not be analyzed.")
        
        # Filter for blobs (files) and check their sizes
        threshold_bytes = threshold_kb * 1024
        
        for item in tree_data.get('tree', []):
            if item.get('type') == 'blob' and item.get('size', 0) > threshold_bytes:
                large_files.append({
                    'path': item.get('path'),
                    'size_bytes': item.get('size'),
                    'size_formatted': format_file_size(item.get('size')),
                    'repo_path': f"{owner}/{repo_name}/{item.get('path')}"
                })
        
        # Sort large files by size (descending) and limit to max_files
        large_files.sort(key=lambda x: x['size_bytes'], reverse=True)
        large_files = large_files[:max_files]
        
        time.sleep(rate_limit_pause)  # Pause to avoid rate limiting
        
        logger.info(f"Found {len(large_files)} large files (>{threshold_kb} KB) in {owner}/{repo_name}")
        return large_files
        
    except Exception as e:
        logger.error(f"Error scanning repository for large files: {e}")
        return []

def scan_repository(headers, token, repo_url, args):
    """Scan a GitHub repository using GraphQL API and collect information."""
    try:
        normalized_repo = normalize_repo_url(repo_url)
        if not normalized_repo:
            return {
                'repository_url': repo_url,
                'status': 'error',
                'error': 'Invalid repository format'
            }
        
        owner, repo_name = normalized_repo.split('/')
        logger.info(f"Scanning repository: {owner}/{repo_name}")
        
        # GraphQL query for repository information
        query = """
        query($owner: String!, $name: String!) {
          repository(owner: $owner, name: $name) {
            name
            owner {
              login
            }
            description
            url
            homepageUrl
            stargazerCount
            forkCount
            isPrivate
            isArchived
            isDisabled
            createdAt
            updatedAt
            pushedAt
            primaryLanguage {
              name
            }
            languages(first: 10, orderBy: {field: SIZE, direction: DESC}) {
              edges {
                node {
                  name
                }
                size
              }
              totalSize
            }
            licenseInfo {
              name
              key
            }
            defaultBranchRef {
              name
              target {
                ... on Commit {
                  history(first: 1) {
                    nodes {
                      committedDate
                      message
                      author {
                        name
                        email
                      }
                    }
                  }
                }
              }
            }
            issues(states: OPEN) {
              totalCount
            }
            pullRequests(states: OPEN) {
              totalCount
            }
            releases(first: 1) {
              nodes {
                name
                tagName
                publishedAt
              }
            }
            codeOfConduct {
              name
            }
            securityPolicyUrl
            diskUsage
            collaborators {
              totalCount
            }
          }
          rateLimit {
            limit
            remaining
            resetAt
          }
        }
        """
        
        variables = {
            'owner': owner,
            'name': repo_name
        }
        
        result = run_graphql_query(headers, query, variables)
        
        # Check if rate limit info is available
        if 'data' in result and 'rateLimit' in result['data']:
            rate_limit = result['data']['rateLimit']
            logger.info(f"Rate limit - Remaining: {rate_limit['remaining']}/{rate_limit['limit']}, Reset at: {rate_limit['resetAt']}")
        
        # Check if repository data is available
        if 'data' not in result or 'repository' not in result['data'] or result['data']['repository'] is None:
            return {
                'repository_url': repo_url,
                'status': 'error',
                'error': 'Repository not found or no access'
            }
        
        repo = result['data']['repository']
        
        # Extract repository data
        repo_data = {
            'repository_url': repo_url,
            'owner': owner,
            'name': repo_name,
            'status': 'success',
            'stars': repo['stargazerCount'],
            'forks': repo['forkCount'],
            'open_issues': repo['issues']['totalCount'],
            'open_pull_requests': repo['pullRequests']['totalCount'],
            'created_at': repo['createdAt'],
            'updated_at': repo['updatedAt'],
            'pushed_at': repo['pushedAt'],
            'primary_language': repo['primaryLanguage']['name'] if repo['primaryLanguage'] else None,
            'license': repo['licenseInfo']['name'] if repo['licenseInfo'] else 'None',
            'license_key': repo['licenseInfo']['key'] if repo['licenseInfo'] else None,
            'private': repo['isPrivate'],
            'archived': repo['isArchived'],
            'disabled': repo['isDisabled'],
            'description': repo['description'],
            'homepage_url': repo['homepageUrl'],
            'disk_usage_kb': repo['diskUsage'],
            'collaborators_count': repo['collaborators']['totalCount'] if repo['collaborators'] else None,
            'has_security_policy': repo['securityPolicyUrl'] is not None,
            'has_code_of_conduct': repo['codeOfConduct']['name'] if repo['codeOfConduct'] else None,
            'default_branch': repo['defaultBranchRef']['name'] if repo['defaultBranchRef'] else None,
        }
        
        # Get latest commit info if available
        if (repo['defaultBranchRef'] and repo['defaultBranchRef']['target'] and 
            'history' in repo['defaultBranchRef']['target'] and 
            repo['defaultBranchRef']['target']['history']['nodes']):
            
            latest_commit = repo['defaultBranchRef']['target']['history']['nodes'][0]
            repo_data.update({
                'last_commit_date': latest_commit['committedDate'],
                'last_commit_message': latest_commit['message'].split('\n')[0],  # First line only
                'last_commit_author': latest_commit['author']['name'],
                'last_commit_email': latest_commit['author']['email'],
            })
        
        # Get latest release info if available
        if repo['releases']['nodes']:
            latest_release = repo['releases']['nodes'][0]
            repo_data.update({
                'latest_release_name': latest_release['name'],
                'latest_release_tag': latest_release['tagName'],
                'latest_release_date': latest_release['publishedAt'],
            })
        
        # Get top languages
        if repo['languages']['edges']:
            total_size = repo['languages']['totalSize']
            for i, lang_edge in enumerate(repo['languages']['edges'][:5]):  # Top 5 languages
                lang_name = lang_edge['node']['name']
                lang_size = lang_edge['size']
                lang_percentage = (lang_size / total_size) * 100 if total_size > 0 else 0
                
                repo_data[f'language_{i+1}_name'] = lang_name
                repo_data[f'language_{i+1}_percentage'] = round(lang_percentage, 2)
        
        # Scan for large files if we have a default branch
        if repo_data['default_branch']:
            large_files = scan_repository_for_large_files(
                token,
                owner,
                repo_name, 
                repo_data['default_branch'],
                args.large_file_threshold,
                args.max_files_per_repo,
                args.rate_limit_pause
            )
            
            # Add large files to the repo data
            repo_data['large_files_count'] = len(large_files)
            
            for i, file in enumerate(large_files):
                index = i + 1
                repo_data[f'large_file_{index}_path'] = file['path']
                repo_data[f'large_file_{index}_size'] = file['size_formatted']
                repo_data[f'large_file_{index}_full_path'] = file['repo_path']
        
        # Pause to avoid rate limiting
        time.sleep(args.rate_limit_pause)
        
        return repo_data
    
    except Exception as e:
        logger.error(f"Error scanning repository {repo_url}: {e}")
        return {
            'repository_url': repo_url,
            'status': 'error',
            'error': str(e)
        }

def write_results(results, output_file):
    """Write scan results to the output CSV file."""
    if not results:
        logger.warning("No results to write")
        return
    
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            # Get all possible fields from all results
            fieldnames = set()
            for result in results:
                fieldnames.update(result.keys())
            
            # Sort fieldnames to ensure consistent column order
            fieldnames = sorted(list(fieldnames))
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for result in results:
                writer.writerow(result)
        
        logger.info(f"Results written to {output_file}")
    except Exception as e:
        logger.error(f"Error writing output file: {e}")

def main():
    """Main function to run the GitHub repository scanner."""
    args = parse_arguments()
    
    # Validate input file
    if not os.path.isfile(args.input):
        logger.error(f"Input file not found: {args.input}")
        return 1
    
    # Create authentication headers
    try:
        headers, token = create_auth_headers(args)
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        return 1
    
    # Read repositories from input file
    repositories = read_repositories(args.input)
    if not repositories:
        logger.error("No repositories found in input file")
        return 1
    
    # Test GraphQL API connection
    try:
        test_query = """
        query {
          viewer {
            login
          }
          rateLimit {
            limit
            remaining
            resetAt
          }
        }
        """
        result = run_graphql_query(headers, test_query)
        if 'data' in result and 'viewer' in result['data']:
            logger.info(f"Successfully authenticated as: {result['data']['viewer']['login']}")
            if 'rateLimit' in result['data']:
                rate_limit = result['data']['rateLimit']
                logger.info(f"Rate limit - Remaining: {rate_limit['remaining']}/{rate_limit['limit']}, Reset at: {rate_limit['resetAt']}")
    except Exception as e:
        logger.error(f"Failed to connect to GitHub GraphQL API: {e}")
        return 1
    
    # Scan repositories
    logger.info(f"Starting repository scan via GraphQL with large file detection (threshold: {args.large_file_threshold} KB)")
    results = []
    for repo_url in repositories:
        result = scan_repository(headers, token, repo_url, args)
        results.append(result)
    
    # Write results to output file
    write_results(results, args.output)
    
    logger.info("Scan completed")
    return 0

if __name__ == "__main__":
    sys.exit(main())

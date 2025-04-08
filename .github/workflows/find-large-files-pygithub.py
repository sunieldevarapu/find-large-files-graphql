name: Find Large Files (PyGitHub)

on:
  workflow_dispatch:

jobs:
  scan:
    runs-on: windows-latest

    env:
      GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}

    steps:
    - name: Checkout repo
      uses: actions/checkout@v3

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: "3.10"

    - name: Install dependencies
      run: pip install PyGithub

    - name: Run large file scanner (PyGitHub)
      run: python find_large_files_pygithub.py

    - name: Upload report
      uses: actions/upload-artifact@v4
      with:
        name: large-files-report
        path: large_files_report.txt

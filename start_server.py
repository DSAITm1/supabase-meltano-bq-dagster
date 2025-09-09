#!/usr/bin/env python3
"""
Startup script for Dagster web server in Cloud Run
"""

import os
import sys
import subprocess
from pathlib import Path

def main():
    """Start the Dagster web server"""
    # Set up environment
    os.environ['DAGSTER_HOME'] = '/app'
    os.environ['PYTHONPATH'] = '/app'
    
    # Change to the bec-dagster directory
    dagster_dir = Path('/app/bec-dagster')
    os.chdir(dagster_dir)
    
    # Start Dagster web server
    cmd = [
        'dagster-webserver',
        '-h', '0.0.0.0',
        '-p', '3000',
        '-w', 'workspace.yaml'
    ]
    
    print(f"Starting Dagster web server with command: {' '.join(cmd)}")
    print(f"Working directory: {os.getcwd()}")
    print(f"Environment variables:")
    for key in ['GOOGLE_CLOUD_PROJECT', 'DATA_PROJECT_ID', 'GOOGLE_APPLICATION_CREDENTIALS']:
        print(f"  {key}: {os.environ.get(key, 'Not set')}")
    
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error starting Dagster web server: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("Shutting down Dagster web server...")
        sys.exit(0)

if __name__ == '__main__':
    main()

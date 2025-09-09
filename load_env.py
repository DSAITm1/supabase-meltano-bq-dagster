#!/usr/bin/env python3
"""
Environment loader that can handle multi-line JSON values
"""
import os
import sys

def load_env_file(env_file_path):
    """Load environment variables from .env file, handling multi-line JSON"""
    with open(env_file_path, 'r') as f:
        content = f.read()
    
    lines = content.split('\n')
    i = 0
    
    while i < len(lines):
        line = lines[i].strip()
        
        # Skip comments and empty lines
        if not line or line.startswith('#'):
            i += 1
            continue
        
        if '=' in line:
            key, value = line.split('=', 1)
            key = key.strip()
            value = value.strip()
            
            # Handle multi-line JSON that starts with a quote and opening brace
            if value == "'{":  # This line just has the opening quote and brace
                # Multi-line JSON - collect all lines until closing brace and quote
                json_lines = ["{"]  # Start with the opening brace
                i += 1
                
                while i < len(lines):
                    current_line = lines[i]
                    
                    if current_line.strip() == "}'":
                        # This is the last line with just closing brace and quote
                        json_lines.append("}")
                        break
                    else:
                        json_lines.append(current_line)
                    i += 1
                
                value = '\n'.join(json_lines)
            else:
                # Remove surrounding quotes for simple values
                if (value.startswith('"') and value.endswith('"')) or \
                   (value.startswith("'") and value.endswith("'")):
                    value = value[1:-1]
            
            # Print the export statement
            # Escape any special characters for shell
            escaped_value = value.replace('"', '\\"').replace('`', '\\`').replace('$', '\\$')
            print(f'export {key}="{escaped_value}"')
        
        i += 1

if __name__ == "__main__":
    env_file = sys.argv[1] if len(sys.argv) > 1 else ".env"
    try:
        load_env_file(env_file)
    except FileNotFoundError:
        print(f"Error: {env_file} not found", file=sys.stderr)
        sys.exit(1)

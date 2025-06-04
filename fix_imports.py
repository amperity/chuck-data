#!/usr/bin/env python3
import os
import re
from pathlib import Path

def fix_imports_in_file(file_path):
    """Fix absolute imports to relative imports in a file."""
    with open(file_path, 'r') as f:
        content = f.read()
    
    original_content = content
    
    # Pattern to match "from src.xyz import ..." 
    pattern = r'from src\.([a-zA-Z_][a-zA-Z0-9_./]*) import'
    
    def replace_import(match):
        module_path = match.group(1)
        # Convert to relative import based on file location
        file_dir = Path(file_path).parent
        src_dir = Path('/Users/johnrush/repos/chuck-data/src/chuck_data')
        
        # Calculate relative path from current file to src root
        try:
            rel_path = os.path.relpath(src_dir, file_dir)
            if rel_path == '.':
                return f'from .{module_path} import'
            else:
                # Count the number of '..' needed
                parts = rel_path.split('/')
                dots = '.' * (parts.count('..') + 1)
                if len(parts) == 1 and parts[0] == '..':
                    return f'from ..{module_path} import'
                else:
                    return f'from {dots}{module_path} import'
        except:
            # Fallback to simple relative import
            return f'from .{module_path} import'
    
    content = re.sub(pattern, replace_import, content)
    
    if content != original_content:
        with open(file_path, 'w') as f:
            f.write(content)
        print(f"Fixed imports in {file_path}")
        return True
    return False

def main():
    src_dir = Path('/Users/johnrush/repos/chuck-data/src/chuck_data')
    
    # Find all Python files
    py_files = list(src_dir.rglob('*.py'))
    
    fixed_count = 0
    for py_file in py_files:
        if fix_imports_in_file(py_file):
            fixed_count += 1
    
    print(f"Fixed imports in {fixed_count} files")

if __name__ == '__main__':
    main()
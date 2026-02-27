#!/usr/bin/env python3
from maintenance import cleanup_old_files

if __name__ == "__main__":
    deleted = cleanup_old_files()
    print(f"deleted={deleted}")

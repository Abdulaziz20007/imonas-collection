#!/usr/bin/env python3
"""
Utility script to check which processes are using ports 3030-3033 and 8080-8081.
This helps identify and kill processes that might be blocking the web server.
"""

import subprocess
import sys

def check_port_usage():
    """Check which processes are using the ports."""
    ports_to_check = [3030, 4040]
    
    print("🔍 Checking port usage...")
    print("-" * 50)
    
    for port in ports_to_check:
        try:
            # Use netstat to check port usage on Windows
            if sys.platform == "win32":
                result = subprocess.run(
                    ["netstat", "-ano", "|", "findstr", f":{port}"],
                    shell=True,
                    capture_output=True,
                    text=True
                )
                if result.returncode == 0 and result.stdout.strip():
                    print(f"🔴 Port {port}: IN USE")
                    lines = result.stdout.strip().split('\n')
                    for line in lines:
                        if f":{port}" in line:
                            parts = line.split()
                            if len(parts) >= 5:
                                pid = parts[-1]
                                print(f"   Process ID: {pid}")
                else:
                    print(f"🟢 Port {port}: Available")
            else:
                # Use lsof for Unix-like systems
                result = subprocess.run(
                    ["lsof", "-i", f":{port}"],
                    capture_output=True,
                    text=True
                )
                if result.returncode == 0 and result.stdout.strip():
                    print(f"🔴 Port {port}: IN USE")
                    print(f"   {result.stdout.strip()}")
                else:
                    print(f"🟢 Port {port}: Available")
                    
        except Exception as e:
            print(f"❌ Error checking port {port}: {e}")
    
    print("-" * 50)
    print("💡 To kill a process: taskkill /PID <PID> /F (Windows) or kill <PID> (Linux/Mac)")

if __name__ == "__main__":
    check_port_usage()

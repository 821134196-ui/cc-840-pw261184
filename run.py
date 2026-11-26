#!/usr/bin/env python3
"""启动迁移台：python3 -I run.py [host] [port]"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from crd_migrator.apiserver import serve

if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8080
    serve(host, port)

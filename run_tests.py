#!/usr/bin/env python3
"""测试入口：python3 -I run_tests.py [-v]"""
import os
import sys
import unittest

# -I 模式下不读 PYTHONPATH，显式把项目根加入模块搜索路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = loader.discover("tests")
    runner = unittest.TextTestRunner(verbosity=2 if "-v" in sys.argv else 1)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)

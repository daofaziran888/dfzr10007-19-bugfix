"""pytest 根配置：保证从仓库任意位置运行都能 import app 包。"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

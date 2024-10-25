import sys
import os

# https://stackoverflow.com/questions/20971619/ensuring-py-test-includes-the-application-directory-in-sys-path
# Make sure that the application source directory (this directory's parent) is
# on sys.path.

here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, here)

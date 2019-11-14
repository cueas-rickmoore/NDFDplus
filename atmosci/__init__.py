try:
    __import__('pkg_resources').declare_namespace(__name__)
except ImportError:
    pass # must not have setuptools

import os
__version__ = open(os.path.join(os.path.split(os.path.abspath(__file__))[0],'version.txt')).read().strip()

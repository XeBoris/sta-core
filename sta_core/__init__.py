"""Top-level package for sta-core."""

__author__ = """Boris Bauermeister"""
__email__ = 'Boris.Bauermeister@gmail.com'
__version__ = '0.1.0'


from .handler.db_handler import DataBaseHandler

from .simple_actions import create_db
from .simple_actions import load_db
from .simple_actions import add_user

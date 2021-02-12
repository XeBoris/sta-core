#!/usr/bin/env python

"""Tests for `sta_core` package."""


import unittest

from sta_core.simple_actions import create_db

class TestSta_core(unittest.TestCase):
    """Tests for `sta_core` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_000_something(self):
        """Test something."""

    def test_create_db(self):

        db_type = "FileDataBase"
        db_path = "/home/koenig/sta-test"
        db_name = "sta-test-db"

        create_db(db_type=db_type,
                  db_path=db_path,
                  db_name=db_name)

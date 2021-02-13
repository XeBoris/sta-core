#!/usr/bin/env python

"""Tests for `sta_core` package."""

import os
import shutil
import unittest

from sta_core.simple_actions import create_db
from sta_core.simple_actions import load_db

class TestSta_core(unittest.TestCase):
    """Tests for `sta_core` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

        self.db_type = "FileDataBase"
        self.db_path = "/home/koenig/sta-test"
        self.db_name = "sta-test-db"


    def tearDown(self):
        """Tear down test fixtures, if any."""
        pass

    def purge_db(self):
        shutil.rmtree(self.db_path)

    def test_000_something(self):
        """Test something."""

    def test_create_db(self):
        self.setUp()
        create_db(db_type=self.db_type,
                  db_path=self.db_path,
                  db_name=self.db_name)

        self.purge_db()

    def test_load_db(self):
        self.setUp()

        create_db(db_type=self.db_type,
                  db_path=self.db_path,
                  db_name=self.db_name)

        load_db(db_type=self.db_type,
                db_path=self.db_path,
                db_name=self.db_name)
        self.purge_db()

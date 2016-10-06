#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import os
import shutil

from pycstbox.config import CONFIG_DIR
from pycstbox.performer.commons.runner import Runner
from pycstbox.performer.commons.analytics import PeriodicAnalyzer, AnalyzerError

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'

__here__ = os.path.dirname(__file__)


class Args(object):
    pass


class ConfigTestCase(unittest.TestCase):
    def test_01_bad_ref(self):
        cfg_path = os.path.join(__here__, 'fixtures/analytics-bad_ref.cfg')

        runner = Runner(config_path=cfg_path, period=PeriodicAnalyzer.PERIOD_DAY)
        with self.assertRaises(AnalyzerError) as cm:
            runner.prepare_analyzers()
        self.assertIn('unresolved definition', cm.exception.message)

    def test_02_mixed(self):
        cfg_path = os.path.join(__here__, 'fixtures/analytics-mixed.cfg')

        runner = Runner(config_path=cfg_path, period=PeriodicAnalyzer.PERIOD_DAY)
        analyzers = runner.prepare_analyzers()
        self.assertEquals(len(analyzers), 2)
        self.assertSetEqual({a[0].__name__ for a in analyzers}, {'WU1', 'WU2'})

    def test_03_missing_module(self):
        cfg_path = os.path.join(__here__, 'fixtures/analytics-missing_module.cfg')

        runner = Runner(config_path=cfg_path, period=PeriodicAnalyzer.PERIOD_DAY)
        with self.assertRaises(AnalyzerError) as cm:
            runner.prepare_analyzers()
        self.assertIn('analyzers_module not configured', cm.exception.message)

    def test_04_skip(self):
        cfg_path = os.path.join(__here__, 'fixtures/analytics-skip.cfg')
        runner = Runner(config_path=cfg_path, period=PeriodicAnalyzer.PERIOD_DAY)
        analyzers = runner.prepare_analyzers()
        self.assertEquals(len(analyzers), 2)
        self.assertSetEqual({a[0].__name__ for a in analyzers}, {'WU1', 'WU3'})


class ConfigRelativePathTestCase(unittest.TestCase):
    import_name = 'wu.cfg'
    cfg_name = 'analytics-mixed.cfg'

    def setUp(self):
        self._clean_cfg_files()

    def tearDown(self):
        self._clean_cfg_files()

    def _clean_cfg_files(self):
        for name in (self.import_name, self.cfg_name):
            os.remove(os.path.join(CONFIG_DIR, name))

    def test_01(self):
        runner = Runner(config_path=self.cfg_name, period=PeriodicAnalyzer.PERIOD_DAY)
        with self.assertRaises(AnalyzerError) as cm:
            runner.prepare_analyzers()
        self.assertIn('No such file or directory', cm.exception.message)

        shutil.copy(os.path.join(__here__, 'fixtures', self.cfg_name), CONFIG_DIR)
        runner = Runner(config_path=self.cfg_name, period=PeriodicAnalyzer.PERIOD_DAY)
        with self.assertRaises(AnalyzerError) as cm:
            runner.prepare_analyzers()
        self.assertIn('imported configuration file not found', cm.exception.message)

        shutil.copy(os.path.join(__here__, 'fixtures', self.import_name), CONFIG_DIR)
        runner = Runner(config_path=self.cfg_name, period=PeriodicAnalyzer.PERIOD_DAY)
        self.assertEquals(len(runner.prepare_analyzers()), 2)


if __name__ == '__main__':
    unittest.main()

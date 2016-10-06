#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import os

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
        self.assertTrue('unresolved definition' in cm.exception.message)

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
        self.assertTrue('analyzers_module not configured' in cm.exception.message)

    def test_04_skip(self):
        cfg_path = os.path.join(__here__, 'fixtures/analytics-skip.cfg')
        runner = Runner(config_path=cfg_path, period=PeriodicAnalyzer.PERIOD_DAY)
        analyzers = runner.prepare_analyzers()
        self.assertEquals(len(analyzers), 2)
        self.assertSetEqual({a[0].__name__ for a in analyzers}, {'WU1', 'WU3'})


if __name__ == '__main__':
    unittest.main()

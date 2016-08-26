#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import os
import datetime
import json

from pycstbox.performer.analytics.runner import Runner
from pycstbox.performer.analytics.base import PeriodicAnalyzer, AnalyzerError

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'

__here__ = os.path.dirname(__file__)


class Args(object):
    pass


class WoopaAnalyticsTestCase(unittest.TestCase):
    TEMP_CFG = '/tmp/analytics.cfg'

    def select_analyzer(self, analyzer_name):
        with open(os.path.join(__here__, 'fixtures/analytics-day_woopa.cfg')) as fp:
            global_config = json.load(fp)

        for cfg in [cfg for cfg in global_config['analyzers'] if cfg['name'] != analyzer_name]:
            cfg['skip'] = True

        with open(self.TEMP_CFG, 'w') as fp:
            json.dump(global_config, fp, indent=4)

    def test_WU1(self):
        self.select_analyzer('WU1')

        runner = Runner()

        args = Args()
        args.period = PeriodicAnalyzer.PERIOD_NAMES[PeriodicAnalyzer.PERIOD_DAY]
        args.loglevel = 'info'
        args.config_path = self.TEMP_CFG
        args.computation_date = datetime.datetime(2016, 7, 17)

        self.assertEquals(runner.main(args), 0)

    def test_WU2(self):
        self.select_analyzer('WU2')

        runner = Runner()

        args = Args()
        args.period = PeriodicAnalyzer.PERIOD_NAMES[PeriodicAnalyzer.PERIOD_DAY]
        args.loglevel = 'info'
        args.config_path = self.TEMP_CFG
        args.computation_date = datetime.datetime(2016, 7, 17)

        self.assertEquals(runner.main(args), 0)

    def test_WU3(self):
        self.select_analyzer('WU3')

        runner = Runner()

        args = Args()
        args.period = PeriodicAnalyzer.PERIOD_NAMES[PeriodicAnalyzer.PERIOD_DAY]
        args.loglevel = 'info'
        args.config_path = self.TEMP_CFG
        args.computation_date = datetime.datetime(2016, 7, 17)

        self.assertEquals(runner.main(args), 0)


if __name__ == '__main__':
    unittest.main()

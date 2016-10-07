#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import datetime
from argparse import ArgumentTypeError

from pycstbox.cli import get_argument_parser, add_config_file_option_to_parser
from pycstbox.performer.commons.analytics import PeriodicAnalyzer
from pycstbox.performer.commons.runner import Runner

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'


if __name__ == '__main__':
    parser = get_argument_parser("PERFORMER periodic analytics")

    def valid_period_name(s):
        if s not in PeriodicAnalyzer.PERIOD_NAMES:
            raise ArgumentTypeError()
        return s

    parser.add_argument(
        '-p', '--period',
        dest='period',
        help='activation periodicity [choices: %s]' % '|'.join(PeriodicAnalyzer.PERIOD_NAMES),
        default=PeriodicAnalyzer.PERIOD_NAMES[PeriodicAnalyzer.PERIOD_DAY],
        type=valid_period_name
    )

    add_config_file_option_to_parser(parser, dflt_name='analytics.cfg', must_exist=True)

    def _valid_iso_date(s):
        try:
            d = datetime.datetime.strptime(s, '%Y-%m-%d').date()
        except ValueError:
            raise ArgumentTypeError('invalid date : %s' % s)
        else:
            return d

    parser.add_argument(
        'computation_date',
        nargs='?',
        type=_valid_iso_date,
        help='the date on which to computation is done, in ISO format'
    )

    args = parser.parse_args()

    sys.exit(Runner.main(args))

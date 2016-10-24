#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from pycstbox.performer.commons.pdw import PDWConnectorMixin


pdw = PDWConnectorMixin(logging.getLogger())
pdw.create_pdw_variable_if_needed(3, 'bar', {
                'type': 'temperature',
                'unit': 'degC'
            }
)

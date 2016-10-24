#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests

s = requests.Session()

definition = [{
                'name': 'foo',
                'type': 'temperature',
                'unit': 'degC'
            }]

req = requests.Request(
    'PUT',
    "http://localhost:8888/vardefs",
    json=definition,
    headers={
        "Content-Type": "application/json",
        "Content-Disposition": "attachment;filename=vardefs.json"
    }
)
prepared = s.prepare_request(req)
reply = s.send(prepared)
try:
    reply.raise_for_status()
except requests.HTTPError as e:
    print('variable creation failure')
else:
    print('variable created')

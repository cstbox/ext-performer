# -*- coding: utf-8 -*-

import requests
import zipfile
import cStringIO
import json

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'


class PDWConnectorMixin(object):
    URL = "http://pdw.performerproject.eu/api/dss/sites/%(site_id)s/%(path)s"

    def __init__(self, logger, report_to=None, dry_run=False, **kwargs):
        self._logger = logger.getChild('pdw')
        self._report_to = report_to
        self._dry_run = dry_run

    def create_pdw_variable_if_needed(self, site_id, var_name, var_meta=None, known_vars=None):
        """ Declares a variable in the PDW if not already there.

        :param int site_id: the id of the site
        :param str var_name: the name of the variable
        :param dict var_meta: meta data of the variable (refer to PDW services specifications, part 5.1 for details)
        :param list known_vars: the list of known variables. If passed, the PDW will not be queried for it.
        :return: the updated known variables list
        :rtype: list
        """
        if not known_vars:
            self._logger.info("getting existing variables list for site id=%s", site_id)
            reply = requests.get(self.URL % {"site_id": site_id, 'path': 'varlist'})
            try:
                reply.raise_for_status()
            except (requests.HTTPError, requests.ConnectionError) as e:
                self._logger.error(e)
                return None
            else:
                known_vars = reply.json()['varlist']
                if known_vars:
                    self._logger.info('--> %d variable(s) already defined', len(known_vars))
                else:
                    self._logger.warn('--> no variable defined yet')

        else:
            if not isinstance(known_vars, list):
                raise TypeError('known_vars parameter type mismatch (expected: list, received: %s)' % type(known_vars))

            self._logger.info("using passed known variables list (len=%d)", len(known_vars))

        if var_name not in known_vars:
            self._logger.info("%s does not exist yet for site id=%d", var_name, site_id)
            if not var_meta:
                msg = 'cannot define variable (meta data not provided)'
                self._logger.error(msg)
                raise ValueError(msg)

            try:
                definition = [{
                    'name': var_name,
                    'type': var_meta['type'],
                    'unit': var_meta.get('unit', 'none'),
                }]
            except KeyError as e:
                msg = 'missing property "%s" in variable meta data' % e
                self._logger.error(msg)
                raise ValueError(msg)

            else:
                request = self.URL % {"site_id": site_id, 'path': 'vardefs'}
                if not self._dry_run:
                    if True:
                        reply = requests.put(
                            request,
                            json=definition,
                            headers={
                                "Content-Type": "application/json",
                                "Content-Disposition": "attachment;filename=vardefs.json"
                            }
                        )
                        try:
                            reply.raise_for_status()
                        except requests.HTTPError as e:
                            msg = 'variable creation failure (%s:%s) : %s' % (site_id, var_name, e)
                            self._logger.error(msg)
                            raise PDWConnectorError(msg)
                        else:
                            self._logger.info('variable created (%s:%s)', site_id, var_name)

                    else:
                        # TODO remove temp workaround if validated
                        import subprocess, os

                        tmp_file = '/tmp/vardefs.json'
                        with open(tmp_file, 'w') as fp:
                            json.dump(definition, fp)
                        try:
                            subprocess.check_call(
                                'curl -X PUT '
                                '-H "Content-Disposition:attachment;filename=vardefs.json" '
                                '-H "Content-Type:application/json" '
                                '-T /tmp/vardefs.json ' +
                                request,
                                shell=True
                            )
                        except subprocess.CalledProcessError as e:
                            msg = 'variable creation failure (%s:%s) : %s' % (site_id, var_name, e)
                            self._logger.error(msg)
                            raise PDWConnectorError(msg)
                        else:
                            self._logger.info('variable created (%s:%s)', site_id, var_name)
                        finally:
                            os.remove(tmp_file)

                else:
                    self._simulate(request, definition)

            # update the known variables list
            known_vars.append(var_name)

        return known_vars

    def _simulate(self, request, data=None):
        self._logger.info('DRY RUN: simulating PUT request : req=%s data=%s', request, data)

    def store_single_points(self, site_id, points, timestamp=None):
        """ Stores a list of points in the PDW.

        ..important:: If the timestamp parameter is provided, it must be compatible to what is accepted by the
        :py:meth:`arrow.get() method from Arrow package (see http://crsmithdev.com/arrow/).

        :param points: an iterable of tuples (var_name, value)
        :param timestamp: the timestamp to be used for the new points. Defaulted to current time
        """
        # ensure the list of points can be traversed several times (we can need this by the end of the method)
        if not isinstance(points, (list, tuple)):
            points = [p for p in points]
        var_names, _ = zip(*points)

        sio = cStringIO.StringIO()
        zf = zipfile.ZipFile(sio, 'w', zipfile.ZIP_DEFLATED)
        for name, value in points:
            zf.writestr(name + ".tsv", '%s\t%s\n' % (timestamp.isoformat(), value))
        zf.close()
        sio.seek(0)

        self._logger.info("storing points for site id=%s: %s", site_id, [(name, value) for name, value in points])

        request = self.URL % {"site_id": site_id, 'path': 'series'}
        if not self._dry_run:
            reply = requests.put(
                request,
                files={
                    'file': sio
                },
                headers={
                    "Content-Type": "application/zip",
                    "Content-Disposition": "attachment;filename=temp.zip"
                }
            )
            try:
                reply.raise_for_status()
            except requests.HTTPError as e:
                msg = '!! failed : %s' % e
                self._logger.error(msg)
                raise PDWConnectorError(msg)
            else:
                self._logger.info('.. success')

        else:
            self._simulate(request)


class PDW(object):
    """ Proxy class for the PERFORMER Data Warehouse """
    URL_BASE = "http://%(host)s/api/dss/"

    def __init__(self, host):
        self._url_base = self.URL_BASE % {'host': host}

    def _make_request_url(self, route):
        return self._url_base + route

    def variables_definition_upload(self, vardefs):
        url = self._make_request_url(vardefs)
        reply = reply = requests.put(
            url=url,
            data=vardefs,
            headers={
                "Content-Type": "application/json"
            }
        )
        reply.raise_for_status()


class PDWConnectorError(Exception):
    pass

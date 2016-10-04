# -*- coding: utf-8 -*-

from pycstbox import evtdb, evtdao, evtmgr

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'

DAO_NAME = 'fsys'


class DataAccessMixin(object):
    """ This mixin provides services for extracting data from a fsys based events DAO """

    def extract_signals(self, time_frame, extracted_variables):
        """ Extract the signals containing the points belonging to the given time frame
        and related to a set of variables.

        Data to be extracted are specified by a dictionary which gives the names of the variables
        which points are requested, and the type of signal to be produced for each one.

        :param TimeFrame time_frame: the definition of the considered time frame
        :param dict extracted_variables: the extraction specification
        """
        try:
            dao_dbus = evtdb.get_object(evtmgr.SENSOR_EVENT_CHANNEL)
            dao_dbus.flush()
        except:
            # we are not on a real CSTBox (test context)
            pass

        dao_direct = evtdao.get_dao(DAO_NAME, readonly=True)

        signals = {}
        var_names = extracted_variables.keys()

        for event in (
                evt for evt in dao_direct.get_events(time_frame.start, time_frame.end)
                if evt.var_name in var_names
        ):
            var_name = event.var_name
            try:
                signal = signals[var_name]
            except KeyError:
                signals[var_name] = signal = extracted_variables[var_name]()

            signal.add_point(event.timestamp, event.value, auto_cast=True)

        return signals

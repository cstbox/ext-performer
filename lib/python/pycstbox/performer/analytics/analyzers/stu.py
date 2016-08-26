# -*- coding: utf-8 -*-

from evtsignals import LogicSignal, AnalogSignal

from pycstbox import evtdao
from pycstbox import evtdb
from pycstbox import evtmgr

from ..base import PeriodicAnalyzer, AbstractIndicator
from .pdw import PDWConnectorMixin

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'

SITE_NAME = "St Teilo"
# TODO put the right value there (would be better via a config file)
SITE_ID = 1

DAO_NAME = 'fsys'


class STU6(PeriodicAnalyzer, PDWConnectorMixin):
    """ Work stations usage analysis """

    #: the delay of the occupancy period triggered by a detected motion
    MOTION_GATE_DELAY = 5 * 60  # seconds

    class Indicator(AbstractIndicator):
        def __init__(self, name, label, description, motion_variable_name, energy_variable_names):
            super(STU6.Indicator, self).__init__(name, label, description)

            self.motion_variable_name = motion_variable_name
            self.energy_variable_names = energy_variable_names

    def __init__(self, indicator, period, computation_date, **kwargs):
        """
        :param STU1.Indicator indicator: the computed indicator definition
        :param int period: analyze period selector (one of AbstractAnalyzer.PERIOD_xxx)
        :param arrow.Arrow computation_date: the reference time to compute the analyzed period.
        If not provided, it is defaulted to now.
        """
        PeriodicAnalyzer.__init__(self,
                                  site_name=SITE_NAME,
                                  indicator=indicator,
                                  period=period,
                                  computation_date=computation_date,
                                  **kwargs
                                  )
        PDWConnectorMixin.__init__(self, self.logger, dry_run=kwargs.get('dry_run', False))

        self.output_varname = "%s_%s" % (self._indicator.name, self.PERIOD_NAMES[self._period])
        self.output_varmeta = {
            'type': 'ratio'
        }
        self.logger.info(" .. output var name : %s", self.output_varname)

    def load_inputs(self, time_frame):
        try:
            dao_dbus = evtdb.get_object(evtmgr.SENSOR_EVENT_CHANNEL)
            dao_dbus.flush()
        except:
            # we are not on a real CSTBox (test context)
            pass

        dao_direct = evtdao.get_dao(DAO_NAME, readonly=True)

        inputs = {}
        signal_classes = {
            self._indicator.motion_variable_name: LogicSignal
        }
        signal_classes.update({
            n: AnalogSignal for n in self._indicator.energy_variable_names
        })
        var_names = signal_classes.keys()

        for event in (
                evt for evt in dao_direct.get_events(time_frame.start, time_frame.end)
                if evt.var_name in var_names
        ):
            var_name = event.var_name
            try:
                signal = inputs[var_name]
            except KeyError:
                inputs[var_name] = signal = signal_classes[var_name]()

            signal.add_point(event.timestamp, event.value, auto_cast=True)

        return inputs

    def create_outputs(self):
        self.create_pdw_variable_if_needed(SITE_ID, self.output_varname, self.output_varmeta)
        return [self.output_varname]

    def store_single_point_outputs(self, timestamp=None):
        self.store_single_points(SITE_ID, self._outputs.iteritems(), timestamp)

    def process_inputs(self, inputs):
        motion_signal = inputs[self._indicator.motion_variable_name]
        energy_signals = [inputs[n] for n in self._indicator.energy_variable_names]

        # derive motion event to produce pseudo-presence signal
        presence_signal = motion_signal.delay(
            duration=self.MOTION_GATE_DELAY * 1000,
            trigger=LogicSignal.EDGE_RAISING,
            restartable=True
        )

        # derive appliances usage by differentiating the cumulative energies, and triggering on non null values
        usage_signals = [LogicSignal((
            (t, v != 0) for t, v in sig.differentiate()
        )) for sig in energy_signals]
        # aggregate them
        global_usage_signal = reduce(lambda x, y: x.logic_or(y), usage_signals)

        # find the misuse periods by ANDing usage and non-presence signals
        misuse_signal = presence_signal.logic_not().logic_and(global_usage_signal)

        # compute the daily ratio of situation occurrences
        period_duration = (self.time_frame.end - self.time_frame.start).total_seconds() * 1000.
        ratio = misuse_signal.integrate(self.time_frame.start, self.time_frame.end) / period_duration

        self.set_output(self.output_varname, ratio)

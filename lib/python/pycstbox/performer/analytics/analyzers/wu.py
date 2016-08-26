# -*- coding: utf-8 -*-

import logging

from evtsignals import LogicSignal, AnalogSignal

from ..base import PeriodicAnalyzer, AbstractIndicator
from .pdw import PDWConnectorMixin

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'


SITE_NAME = "WOOPA"
SITE_ID = 3

DAO_TYPE = 'simul.dao'

if DAO_TYPE == 'simul.dao':
    from woopa import CSTBox_FSys_DAO as DAO

elif DAO_TYPE == 'simul.obix':
    from woopa import Simulated_OBIX_DAO as DAO

else:
    class OBIX_DAO(object):
        def get_events(self, from_time, to_time):
            """ Returns a collection of timed events corresponding to the provided time span.

            :return: collection of :py:class:`TimedEvent`
            :rtype: iterable
            """
            pass

    DAO = OBIX_DAO


class WU_Base(PeriodicAnalyzer, PDWConnectorMixin):
    def __init__(self, indicator, period, computation_date, **kwargs):
        PeriodicAnalyzer.__init__(self,
                                  site_name=SITE_NAME,
                                  indicator=indicator,
                                  period=period,
                                  computation_date=computation_date,
                                  **kwargs
                                  )
        PDWConnectorMixin.__init__(self, **kwargs)


class WU1(WU_Base):
    """ Room occupancy analysis """

    #: the delay of the occupancy period triggered by a detected motion
    MOTION_GATE_DELAY = 5 * 60  # seconds

    class Indicator(AbstractIndicator):
        def __init__(self, name, label, description, motion_variable_names):
            super(WU1.Indicator, self).__init__(name, label, description)

            self.motion_variable_names = motion_variable_names

    def __init__(self, indicator, period, computation_date, **kwargs):
        """
        :param WU1.Indicator indicator: the computed indicator definition
        :param int period: analyze period selector (one of AbstractAnalyzer.PERIOD_xxx)
        :param arrow.Arrow computation_date: the reference time to compute the analyzed period.
        If not provided, it is defaulted to now.
        """
        super(WU1, self).__init__(indicator, period, computation_date, **kwargs)

        self.output_variables = {
            "%s_%s" % (self._indicator.name, self.PERIOD_NAMES[self._period]): {
                'type': 'ratio'
            }
        }

    def load_inputs(self, time_frame):
        dao = DAO()

        inputs = {}
        signal_classes = {
            n: LogicSignal for n in self._indicator.motion_variable_names
        }
        var_names = signal_classes.keys()

        for event in (
                evt for evt in dao.get_events(time_frame.start, time_frame.end)
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
        for name, meta in self.output_variables.iteritems():
            self.create_pdw_variable_if_needed(SITE_ID, name, meta)
        return self.output_variables.keys()

    def store_single_point_outputs(self, timestamp=None):
        self.store_single_points(SITE_ID, self._outputs.iteritems(), timestamp)

    def process_inputs(self, inputs):
        motion_signals = [inputs[n] for n in self._indicator.motion_variable_names]

        # derive motion events to produce pseudo-presence signals
        presence_signals = [s.delay(
            duration=self.MOTION_GATE_DELAY * 1000,
            trigger=LogicSignal.EDGE_RAISING,
            restartable=True
        ) for s in motion_signals]

        # aggregate them
        aggregated_presence_signal = reduce(lambda x, y: x.logic_or(y), presence_signals)

        # compute the daily ratio of presence periods
        period_duration = (self.time_frame.end - self.time_frame.start).total_seconds() * 1000.
        ratio = aggregated_presence_signal.integrate(self.time_frame.start, self.time_frame.end) / period_duration

        self.set_output(self.output_variables.keys()[0], ratio)


class WU2(WU_Base):
    """ Shading and artificial lighting usage correlation analysis """

    RE_SAMPLE_PERIOD = 60 * 5   # seconds
    DEFAULT_SHADE_ATTENUATION_THRESHOLD = 0.8       # lux_ext / lux_shader ratio
    DEFAULT_LIGHTING_ON_LUX_THRESHOLD = 2500        # spotting neon lighting appliance from 0.5m approx.
    DEFAULT_NATURAL_LIGHTING_OK_THRESHOLD = 3000    # self-sufficient outdoor lighting level

    class Indicator(AbstractIndicator):
        def __init__(self, name, label, description,
                     lux_out_ref_name, lux_shade_name, lux_lighting_name,
                     shade_attenuation_threshold=None,
                     lighting_on_lux_threshold=None,
                     natural_lighting_ok_threshold=None
                     ):
            super(WU2.Indicator, self).__init__(name, label, description)

            self.lux_out_ref_name = lux_out_ref_name
            self.lux_shade_name = lux_shade_name
            self.lux_lighting_name = lux_lighting_name
            self.shade_attenuation_threshold = shade_attenuation_threshold or WU2.DEFAULT_SHADE_ATTENUATION_THRESHOLD
            self.lighting_on_lux_threshold = lighting_on_lux_threshold or WU2.DEFAULT_LIGHTING_ON_LUX_THRESHOLD
            self.natural_lighting_ok_threshold = natural_lighting_ok_threshold or WU2.DEFAULT_NATURAL_LIGHTING_OK_THRESHOLD

    def __init__(self, indicator, period, computation_date,
                 **kwargs):
        """
        :param WU2.Indicator indicator: the computed indicator definition
        :param int period: analyze period selector (one of AbstractAnalyzer.PERIOD_xxx)
        :param arrow.Arrow computation_date: the reference time to compute the analyzed period.
        If not provided, it is defaulted to now.
        """
        super(WU2, self).__init__(indicator, period, computation_date, **kwargs)

        self.output_variables = {
            "%s_%s" % (self._indicator.name, self.PERIOD_NAMES[self._period]): {
                'type': 'ratio'
            }
        }

    def load_inputs(self, time_frame):
        dao = DAO()

        required_inputs = {
            self._indicator.lux_out_ref_name,
            self._indicator.lux_shade_name,
            self._indicator.lux_lighting_name
        }
        inputs = {}
        signal_classes = {n: AnalogSignal for n in required_inputs}
        var_names = signal_classes.keys()

        for event in (
                evt for evt in dao.get_events(time_frame.start, time_frame.end)
                if evt.var_name in var_names
        ):
            var_name = event.var_name
            try:
                signal = inputs[var_name]
            except KeyError:
                inputs[var_name] = signal = signal_classes[var_name]()

            signal.add_point(event.timestamp, event.value, auto_cast=True)

        missing_inputs = required_inputs - set(inputs.keys())
        if missing_inputs:
            self.logger.warning('missing data for required input(s) : %s', ', '.join(missing_inputs))
        else:
            return inputs

    def create_outputs(self):
        for name, meta in self.output_variables.iteritems():
            self.create_pdw_variable_if_needed(SITE_ID, name, meta)
        return self.output_variables.keys()

    def store_single_point_outputs(self, timestamp=None):
        self.store_single_points(SITE_ID, self._outputs.iteritems(), timestamp)

    def process_inputs(self, inputs):
        ind = self._indicator

        # re-sample natural light signals to sync them for subsequent delta computation
        for n in (ind.lux_out_ref_name, ind.lux_shade_name):
            inputs[n].extend(directions=AnalogSignal.EXTEND_BOTH)

        re_sampled_signals = {
            n: [pt for pt in inputs[n].re_sample(self.RE_SAMPLE_PERIOD, t_start=self.time_frame.start)]
            for n in (ind.lux_out_ref_name, ind.lux_shade_name)
        }

        # compute lux_out_ref - lux_shade to infer shade position
        sig_lux_out_ref = re_sampled_signals[ind.lux_out_ref_name]
        sig_lux_shade = re_sampled_signals[ind.lux_shade_name]
        sig_shade_attenuation = AnalogSignal(
            (
                (out.timestamp, shade.value / out.value) for out, shade in zip(sig_lux_out_ref, sig_lux_shade)
            )
        )
        # infer shading state on by thresholding the lux levels distance
        sig_shade_closed = sig_shade_attenuation.trigger(ind.shade_attenuation_threshold)

        # infer lighting on by thresholding the lux level in the vicinity of the source
        sig_lights_on = inputs[ind.lux_lighting_name].trigger(ind.lighting_on_lux_threshold)

        # infer sufficient natural lighting periods by thresholding the outdoor lux level
        sig_natural_light_ok = inputs[ind.lux_out_ref_name].trigger(ind.natural_lighting_ok_threshold)

        # identify simultaneous occurrences of conditions
        sig_result = sig_shade_closed.logic_and(sig_lights_on).logic_and(sig_natural_light_ok)

        # compute the final indicator based on the per day occurrence duration ratio
        period_duration = (self.time_frame.end - self.time_frame.start).total_seconds() * 1000.
        ratio = sig_result.integrate(self.time_frame.start, self.time_frame.end) / period_duration

        self.set_output(self.output_variables.keys()[0], ratio)


class WU3(WU_Base):
    """ Effects of windows open state on room temperature """
    TEMPERATURE_ANALYSIS_DELAYS = (10, 15, 30)  # minutes

    class Indicator(AbstractIndicator):
        def __init__(self, name, label, description,
                     window_state_names, room_temperature_name
                     ):
            super(WU3.Indicator, self).__init__(name, label, description)

            self.window_state_names = window_state_names
            self.room_temperature_name = room_temperature_name

    def __init__(self, indicator, period, computation_date,
                 **kwargs):
        """
        :param WU1.Indicator indicator: the computed indicator definition
        :param int period: analyze period selector (one of AbstractAnalyzer.PERIOD_xxx)
        :param arrow.Arrow computation_date: the reference time to compute the analyzed period.
        If not provided, it is defaulted to now.
        """
        super(WU3, self).__init__(indicator, period, computation_date, **kwargs)

        # we keep the variable names in this list so that it is synchronized with temperature analysis delays list
        # (needed at the end of the analysis process)
        self.output_variables_names = [
            "%s_%d_%s" % (self._indicator.name, d, self.PERIOD_NAMES[self._period])
            for d in self.TEMPERATURE_ANALYSIS_DELAYS
        ]
        self.output_variables = {
            n: {
                'type': 'temperature',
                'unit': 'degC'
            } for n in self.output_variables_names
        }

    def load_inputs(self, time_frame):
        dao = DAO()

        inputs = {}
        signal_classes = {
            n: LogicSignal for n in self._indicator.window_state_names
        }
        signal_classes[self._indicator.room_temperature_name] = AnalogSignal
        var_names = signal_classes.keys()

        for event in (
                evt for evt in dao.get_events(time_frame.start, time_frame.end)
                if evt.var_name in var_names
        ):
            var_name = event.var_name
            try:
                signal = inputs[var_name]
            except KeyError:
                inputs[var_name] = signal = signal_classes[var_name]()

            signal.add_point(event.timestamp, event.value, auto_cast=True)

        if self._indicator.room_temperature_name not in inputs:
            missing_inputs = {self._indicator.room_temperature_name}
        elif not set(self._indicator.window_state_names) & set(inputs.keys()):
            missing_inputs = self._indicator.window_state_names
        else:
            missing_inputs = None

        if missing_inputs:
            self.logger.warning('missing data for required input(s) : %s', ', '.join(missing_inputs))
        else:
            return inputs

    def create_outputs(self):
        for name, meta in self.output_variables.iteritems():
            self.create_pdw_variable_if_needed(SITE_ID, name, meta)
        return self.output_variables_names

    def store_single_point_outputs(self, timestamp=None):
        self.store_single_points(SITE_ID, self._outputs.iteritems(), timestamp)

    def process_inputs(self, inputs):
        ind = self._indicator

        temp_signal = inputs[ind.room_temperature_name]
        window_signal = reduce(lambda agg, s: agg.logic_or(s), (inputs[n] for n in self._indicator.window_state_names))
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("window_signal=%s", window_signal)

            self.save_signal_plots(
                [inputs[n] for n in self._indicator.window_state_names] + [window_signal],
                self._indicator.window_state_names + ['aggregated opened'],
                'WU3 aggregated opened'
            )

        # compute the temperature variations for each opening event
        variations = []
        when = []
        for ts, _ in (pt for pt in window_signal if pt.value):
            when.append(ts)
            temp_0 = temp_signal.get_value_at(ts, interpolate=True)
            variations.append([
                temp_signal.get_value_at(ts + delay * 60000, interpolate=True) - temp_0
                for delay in self.TEMPERATURE_ANALYSIS_DELAYS
            ])

        if self.logger.isEnabledFor(logging.DEBUG):
            import datetime
            self.logger.debug(zip((datetime.datetime.utcfromtimestamp(t / 1000.).strftime('%d/%m %H:%M:%S') for t in when), variations))

        # compute the averages over the analyzed time span
        count = len(variations)
        variation_averages = [sum(v) / count for v in zip(*variations)]

        for n, v in zip(self.output_variables_names, variation_averages):
            self.set_output(n, v)

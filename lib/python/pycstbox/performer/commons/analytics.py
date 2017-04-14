# -*- coding: utf-8 -*-

""" Base definitions shared by usage analyzers implementations.
"""

import logging
import os

import arrow

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'

default_logger = logging.getLogger('tsserver').getChild(__name__)


class TimeFrame(object):
    """ The definition of an immutable time frame
    """
    def __init__(self, start, end):
        """
        Either bound can be omitted but not both. Bounds can be provided as ints, floats,
        strings that convert to a float or valid timestamps string representation.

        Refer to the `arrow` package for details of accepted input values.

        :param start: starting time
        :param end: ending time
        :raise ValueError: if start and end are not in the correct sequence if both provided,
        or are both omitted
        :raise TypeError: if start or end and not a datetime or a compatible instance
        """
        if not start and not end:
            raise ValueError('at least one bound must be provided')
        if start:
            start = arrow.get(start).naive
        else:
            start = arrow.get(0).naive
        if end:
            end = arrow.get(end).naive
        else:
            end = arrow.get(10**10).naive     # somewhere far in the future (Sept 2286...)
        if start and end and start >= end:
            raise ValueError('start and end out of sequence')

        self._start = start
        self._end = end

    @property
    def start(self):
        """ Time frame start time as an arrow instance """
        return self._start

    @property
    def end(self):
        """ Time frame end time as an arrow instance """
        return self._end


class AbstractIndicator(object):
    """ Root class for defining and indicator.

    It misses the information describing the inputs used by the indicator, which
    are defined in its sub-classes.
    """
    def __init__(self, name, label, description):
        self.name = name
        self.label = label
        self.description = description
        self.inputs = None

    def check_mandatory_parameters(self, names):
        missing = []
        for name in names:
            if not getattr(self, name, None):
                missing.append(name)
        if missing:
            raise AnalyzerError('missing mandatory parameters [%s]' % (', '.join(missing)))

    def __str__(self):
        return self.name


class AbstractAnalyzer(object):
    """ An analyzer wraps the process computing an indicator.

    This class serves as the foundation for concrete analyzers, which must provide
    a real implementation of the method :py:meth:`process_signals`.
    """
    output_as_series = False

    def __init__(self,
                 indicator, time_frame,
                 logger=None,
                 dry_run=False, save_plots_to=None
                 ):
        """
        :param AbstractIndicator indicator: the definition of the indicator elaborated by the analyzer
        :param TimeFrame time_frame: the analysis time frame

        :raise ValueError: if mandatory parameters are not provided, or are invalid
        :raise TypeError: in case of parameters type mismatch
        """
        self.logger = logger or default_logger

        if not indicator:
            raise ValueError('indicator parameter is mandatory')
        if not time_frame:
            raise ValueError('time_frame parameter is mandatory')
        if not isinstance(time_frame, TimeFrame):
            raise TypeError('invalid time_frame value')
        if save_plots_to and not os.path.isdir(save_plots_to):
            raise ValueError('not found or not a directory : %s' % save_plots_to)

        self._time_frame = time_frame
        self._indicator = indicator

        self._outputs = None

        self.dry_run = dry_run
        self.save_plots_to = save_plots_to

    @property
    def time_frame(self):
        return self._time_frame

    def load_inputs(self, time_frame):
        raise NotImplementedError()

    def create_outputs(self):
        raise NotImplementedError()

    def set_output(self, name, value):
        self._outputs[name] = value

    def run(self, outputs_timestamp=None):
        """ Process the data selected by instantiation parameters, based on the computation
        implemented by :py:meth:`process_signals`.

        By default, the outputs of the processing specific to the analyzer is appended as
        points to the time series associated to them.

        :param outputs_timestamp: the timestamp to be used for recording the outputs in their
        associated time series. If not provided, it is defaulted to now

        :raise AnalyzerError: in case of error during process
        """

        self.logger.info(
            '%s analyzing period [%s, %s]', self.__class__.__name__, self._time_frame.start, self._time_frame.end
        )

        self.logger.info('loading inputs')
        inputs = self.load_inputs(TimeFrame(self._time_frame.start, self._time_frame.end))
        if inputs:
            if self.save_plots_to:
                self.logger.info('plotting input signal(s)')
                labels, signals = zip(*inputs.iteritems())
                self.save_signal_plots(signals, labels)

            self.logger.info('creating outputs')
            self._outputs = {name: None for name in self.create_outputs()}

            self.logger.info('computing indicator(s)')
            self.process_inputs(inputs)

            self.logger.info('storing results (if any)')
            if self.output_as_series:
                self.store_time_series_outputs()
            else:
                self.store_single_point_outputs(outputs_timestamp)
        else:
            self.logger.warn('cannot compute indicator : no input data')

    def process_inputs(self, inputs):
        """ The heart of the analyzer, which does the real job.

        Concrete classes must define this method.

        Processing errors can be reported by raising a :py:class:`AnalyzerError` exception
        (or a subclass of it).

        It the analyze produces outputs, concrete implementations must return them as
        a dictionary of values, keyed by the output name. If not output is produced,
        just return nothing. Such a use case is valid if the signal processing takes care
        of what should be done with its results and do not need the default handling.

        :param dict inputs: the input signals time series, keyed by signal name
        :raise AnalyzerError: in case of processing error
        """
        raise NotImplementedError()

    def store_single_point_outputs(self, timestamp=None):
        """ Stores the outputs of the analyzer (single points).

        This method is used when the outputs of the analyzer are a single point per output
        variable. For outputs in the form of time series, see :py:meth:`store_time_series_outputs`.

        The data are provided as a dictionary of the values, keyed by the output names.
        The points are dated based on the `timestamp` parameter, which is set to the current time
        if not provided. If provided, it must be compatible to what is accepted by the
        :py:meth:`arrow.get() method from Arrow package (see http://crsmithdev.com/arrow/).

        :param timestamp: the optional outputs timestamp
        """
        raise NotImplementedError()

    def store_time_series_outputs(self):
        """ Stores the outputs of the analyzer (time series).

        This method is used when the outputs of the analyzer are a time series per output
        variable. For outputs in the form of single points, see :py:meth:`store_single_point_outputs`.

        The data are provided as a dictionary of the time series, keyed by the output names. The
        individual time series are expected as a list of :py:class:`evtsignals.base.Point` or compatible tuples.
        """
        raise NotImplementedError()

    def save_signal_plots(self, signals, signal_labels, title=None):
        if not self.save_plots_to:
            return

        from evtsignals.plot import plot_signals, matplotlib
        if matplotlib:
            title = title or self.__class__.__name__
            plot_signals(
                signals,
                title="%s (generated on %s)" % (title, arrow.utcnow().strftime('%Y/%m/%d %H:%M:%S')),
                logger=self.logger,
                signal_labels=signal_labels,
                save_as=os.path.join(self.save_plots_to, '%s.png' % title.replace(' ', '_'))
            )
        else:
            self.logger.warn('cannot generate plot (matplotlib not available)')


class AnalyzerError(Exception):
    """ Specialized exception for identifying analyzer processing errors """


class PeriodicAnalyzer(AbstractAnalyzer):
    PERIOD_DAY, PERIOD_WEEK, PERIOD_MONTH = range(3)
    PERIOD_NAMES = ['day', 'week', 'month']

    PERIOD_ANY = 'any'

    """ An analyzer designed to run on a periodic basis, and which analyzes fixed length time
    frame (day, week, month) """
    def __init__(self, site_name, indicator, period=PERIOD_DAY, computation_date=None, **kwargs):
        """
        :param str site_name: the name of the site the analyzed variables belong too
        :param AbstractIndicator indicator: the computed indicator definition
        :param int period: analyze period selector (one of AbstractAnalyzer.PERIOD_xxx)
        :param arrow.Arrow computation_date: the reference date to compute the analyzed period.
        If not provided, it is defaulted to now.
        """
        if not computation_date:
            computation_date = arrow.utcnow()
        else:
            computation_date = arrow.get(computation_date)
        self._computation_date = computation_date.replace(days=-1)

        self._period = period

        period_name = self.PERIOD_NAMES[period]
        tf = TimeFrame(self._computation_date.floor(period_name), self._computation_date.ceil(period_name))

        super(PeriodicAnalyzer, self).__init__(indicator=indicator, time_frame=tf, **kwargs)

    @staticmethod
    def period_name_to_id(name):
        try:
            return PeriodicAnalyzer.PERIOD_NAMES.index(name)
        except IndexError:
            raise ValueError(name)

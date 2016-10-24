# -*- coding: utf-8 -*-

import os
import json
import importlib
import datetime
import logging

from pycstbox import log
from pycstbox.config import CONFIG_DIR

from pycstbox.performer.commons.analytics import PeriodicAnalyzer, AnalyzerError

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'


class Runner(object):
    def __init__(self, config_path, period=PeriodicAnalyzer.PERIOD_DAY, logger=None):
        if not config_path:
            raise ValueError('missing config_path parameter')
        if not os.path.isabs(config_path):
            config_path = os.path.join(CONFIG_DIR, config_path)
        self.config_path = config_path

        if period is None:
            raise ValueError('missing period parameter')

        self.period = period
        
        self.logger = logger or log.getLogger(self.__class__.__name__)
        self.log_info = self.logger.info
        self.log_warn = self.logger.warn
        self.log_error = self.logger.error
        self.log_exception = self.logger.exception

    def prepare_analyzers(self):
        analyzers = []

        try:
            self.log_info('loading configuration from %s', self.config_path)
            cfg_data = json.load(open(self.config_path))

        except Exception as e:
            raise AnalyzerError("configuration error : %s" % e)

        defaults = cfg_data.get('defaults', {})
        default_analyzer_params = defaults.get('analyzer_params', {}) if defaults else {}
        if default_analyzer_params:
            self.log_info('default analyzers parameters : %s', default_analyzer_params)

        cfg_analyzers_module = defaults.get("analyzers_module", None)

        try:
            imported_config = cfg_data['import']
        except KeyError:
            imported_analyzers = {}
        else:
            imported_config_path = os.path.join(os.path.dirname(self.config_path), imported_config)
            if os.path.exists(imported_config_path):
                self.log_info('importing definitions from %s', imported_config_path)
                try:
                    imported_analyzers = json.load(open(imported_config_path))
                except ValueError as e:
                    raise AnalyzerError('invalid imported definitions (%s)' % e)
            else:
                raise AnalyzerError('imported configuration file not found: %s' % imported_config_path)

        for cfg_item in cfg_data['analyzers']:
            name = cfg_item['name']

            if not cfg_item.get('skip', False):
                self.log_info('configuring analyzer %s', name)

                ref = cfg_item.get('ref', None)
                if ref:
                    try:
                        effective_cfg = imported_analyzers[ref]
                    except KeyError:
                        raise AnalyzerError('unresolved definition reference: %s' % ref)
                    effective_cfg.update(cfg_item)
                else:
                    effective_cfg = cfg_item

                analyzer_cfg = effective_cfg['analyzer']
                cfg_fqcn = analyzer_cfg['class']
                if '.' not in cfg_fqcn:
                    # using not fully qualified class names => we need the parent module
                    if cfg_analyzers_module:
                        cfg_fqcn = cfg_analyzers_module + '.' + cfg_fqcn
                    else:
                        raise AnalyzerError(
                            'relative class name used (%s) and analyzers_module not configured' % cfg_fqcn
                        )

                analyzer_fqcn = cfg_fqcn.split('.')
                module_name = '.'.join(analyzer_fqcn[:-1])
                try:
                    self.log_info('importing analyzer class module (%s)', module_name)
                    analyzer_module = importlib.import_module(module_name)
                    class_name = analyzer_fqcn[-1]
                    try:
                        self.log_info('getting analyzer class (%s)', class_name)
                        analyzer_class = getattr(analyzer_module, class_name)
                    except AttributeError as e:
                        raise AnalyzerError('analyzer class not found (%s)' % class_name)
                    else:
                        try:
                            analyzer_indicator_class = getattr(analyzer_class, 'Indicator')
                        except AttributeError:
                            raise AnalyzerError(
                                "invalid analyzer implementation : missing Indicator nested class definition"
                            )
                        else:
                            indicator_params = {k: effective_cfg[k] for k in ('name', 'label', 'description')}
                            indicator_params.update(effective_cfg['indicator_params'])
                            if self.logger.isEnabledFor(logging.INFO):
                                self.log_info('.. indicator parameters :')
                                for k, v in (
                                        (k, v) for k, v in indicator_params.iteritems()
                                        if k not in ('name', 'description', 'label')
                                ):
                                    self.log_info('   + %s = %s', k, v)

                            try:
                                indicator = analyzer_indicator_class(**indicator_params)
                            except Exception as e:
                                self.logger.error(e)
                                raise AnalyzerError('indicator creation error')
                            else:
                                analyzer_params = default_analyzer_params.copy()
                                analyzer_params.update(analyzer_cfg.get('params', {}))

                                analyzers.append((analyzer_class, analyzer_params, indicator))

                except KeyError as e:
                    raise AnalyzerError('missing key "%s" in configuration %s' % (e, cfg_item))

                except ImportError as e:
                    raise AnalyzerError('error importing module %s (%s)' % (module_name, e))

                except AnalyzerError:
                    raise

                except Exception as e:
                    raise AnalyzerError('unexpected error : %s' % e)

            else:
                self.log_warn('!! skipping analyzer %s', name)

        return analyzers

    def execute_analyzers(self, analyzers, computation_date=None):
        computation_date = computation_date or datetime.datetime.utcnow()
        executed = 0
        in_error = 0
        for analyzer_class, analyzer_params, indicator in analyzers:
            self.log_info('processing indicator : %s', indicator.name)
            executed += 1
            try:
                self.log_info('.. initialization parameters :')
                self.log_info('.. + period = %s', PeriodicAnalyzer.PERIOD_NAMES[self.period])
                self.log_info('.. + computation_date = %s', computation_date)
                for k, v in analyzer_params.iteritems():
                    self.log_info('.. + %s = %s', k, v)

                analyzer = analyzer_class(
                    indicator,
                    self.period,
                    computation_date,
                    logger=self.logger.getChild(indicator.name),
                    **analyzer_params
                )
                self.log_info('.. elaboration')
                analyzer.run(outputs_timestamp=computation_date)

            except AnalyzerError as e:
                self.log_error('** analyzer error : %s', e)
                in_error += 1
            except Exception as e:
                self.log_exception('** unexpected error : %s', e)
                in_error += 1
            else:
                self.log_info('!! done.')

        if in_error:
            raise AnalyzerError('%d indicator(s) computation completed with %s error(s)' % (executed, in_error))

    @staticmethod
    def main(args):
        logger = log.getLogger('analytics-%s' % args.period)
        log.set_loglevel_from_args(logger, args)

        def die(msg):
            logger.fatal(msg)
            return msg

        try:
            logger.info('creating runner')
            runner = Runner(
                config_path=args.config_path,
                period=PeriodicAnalyzer.period_name_to_id(args.period)
            )

            logger.info('preparing analyzers')
            analyzers = runner.prepare_analyzers()

        except AnalyzerError as e:
            return die("analysis preparation error (%s)" % e)

        except Exception as e:
            return die("unexpected error : %s" % e)

        else:
            try:
                logger.info('running analyzers')
                runner.execute_analyzers(analyzers, args.computation_date)

            except AnalyzerError as e:
                logger.error(e)
                return die("analyze execution error (%s)" % e)

            else:
                logger.info('completed without error')
                return 0

import logging
import os
import re


"""
We are careful to not depend on other third-party libraries here and to import
and use this module before importing other modules to ensure that no one else
runs :meth:`logging.basicConfig` before this gets a chance to. Otherwise,
it might have no effect here.
"""


default_log_format = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'


def configure_logging():
    """
    If the ``LOGCONFIG`` env. variable is set to the location of a YAML file
    describing a Python logging dict config, that will be used. Otherwise,
    ``LOGFILE`` must be set to a log file location to be
    passed to :meth:`logging.basicConfig`, optionally with a value for
    ``LOGLEVEL`` (defaults to "WARN").

    :return: list of file handles which need to be kept open (objects with a
             ``fileno`` method). The python-daemon package closes file handles
             that you don't pass in through ``files_preserve``.
    """
    def pathex_constructor(loader, node):
        """For substituting environment variables into the YAML log config"""
        value = loader.construct_scalar(node)
        p1, env_var, p2 = pattern.match(value).groups()
        return p1 + os.environ[env_var] + p2

    logconfig = os.environ.get('LOGCONFIG', None)
    if logconfig is not None and logconfig != '':
        try:
            import yaml
        except ImportError:
            raise ImportError('Please install PyYAML (could not import yaml)')
        # Configure env. var. substitution for YAML.
        # http://stackoverflow.com/a/27232341/1118576
        pattern = re.compile(r'^(.*)\<%= ENV\[\'(.*)\'\] %\>(.*)$')
        yaml.add_implicit_resolver("!pathex", pattern)
        yaml.add_constructor('!pathex', pathex_constructor)
        with open(logconfig) as configf:
            config = yaml.load(configf)
        logging.config.dictConfig(config)
    else:
        logfilename = os.environ.get('LOGFILE')
        loglevel_name = os.environ.get('LOGLEVEL', 'WARN')
        loglevel = getattr(logging, loglevel_name.upper())
        logging.basicConfig(filename=logfilename, level=loglevel,
                            format=default_log_format)
    loggers = [logging.getLogger()] + \
        list(logging.Logger.manager.loggerDict.values())
    return [h.stream
            for l in loggers
            if not isinstance(l, logging.PlaceHolder)
            for h in l.handlers
            if isinstance(h, logging.FileHandler)]

"""
DELL EMC ECS API Data Collection Module.
"""
import abc
import logging
import os
from logging.handlers import RotatingFileHandler

DEFAULT_LOG_FILE_NAME = "ecs-pulse.log"


class _Logger(object):
    """
    The base class for logger, all loggers have to extend this class
    and provide implementation for the basic logging methods.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def debug(self, msg):
        pass

    @abc.abstractmethod
    def info(self, msg):
        pass

    @abc.abstractmethod
    def warning(self, msg):
        pass

    @abc.abstractmethod
    def error(self, msg):
        pass


class ECSLogger(_Logger):
    _PREFIX = '[DellEMCECSDataCollection] '

    def __init__(self, module_name, logging_level, log_file=DEFAULT_LOG_FILE_NAME):
        fullLogFilePath = os.path.abspath(os.path.join(os.getcwd(), DEFAULT_LOG_FILE_NAME))
        handler = RotatingFileHandler(log_file, maxBytes=1024*1024, backupCount=100)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s : %(message)s')
        handler.setFormatter(formatter)
        handler.setLevel(logging_level)
        self.logger = logging.getLogger(module_name)
        self.logger.propagate = False
        self.logger.setLevel(logging_level)
        self.logger.addHandler(handler)

    def debug(self, msg):
        self.logger.debug(ECSLogger._PREFIX + msg)

    def info(self, msg):
        self.logger.info(ECSLogger._PREFIX + msg)

    def warning(self, msg):
        self.logger.warning(ECSLogger._PREFIX + msg)

    def error(self, msg):
        self.logger.error(ECSLogger._PREFIX + msg)


def get_logger(module_name=None, logging_level=logging.INFO, log_file=DEFAULT_LOG_FILE_NAME):
    """
    Provides the default logger for the application.
    """
    return ECSLogger(module_name, logging_level, log_file)
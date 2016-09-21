'''
Created on 22 Sep 2016

@author: mozat
'''
import logging


DEFAULT_LOG_FILE = 'logger_video_downloader'
logger = logging.getLogger()
logger.setLevel(logging.WARN)
ch = logging.FileHandler(DEFAULT_LOG_FILE + "_world")
ch.setLevel(logging.DEBUG)
ch.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s -%(message)s"))
logger.addHandler(ch)

#"application" code
logger.debug("debug message")
logger.info("info message")
logger.warn("warn message")
logger.error("error message")
logger.critical("critical message")


logger1 = logging.getLogger("test")
logger1.setLevel(logging.DEBUG)
logger1.debug("debug message")
filename = "test"
print "start download: %s"%filename
import os
import sys
from logging import StreamHandler, Formatter


class LogHandler(StreamHandler):
    """LogHandler class manages log formatting and log output

    Args:
        StreamHandler (class): Class to set logs output
    """

    def __init__(self) -> None:
        output_type = os.getenv("LOGGING_TYPE") or sys.stdout
        StreamHandler.__init__(self, output_type)
        output_format = "%(asctime)s [%(threadName)10s][%(module)10s][%(lineno)4s]\
        [%(levelname)8s] %(message)s"
        format_date = "%Y-%m-%dT%H:%M:%S%Z"
        formatter = Formatter(output_format, format_date)
        self.setFormatter(formatter)
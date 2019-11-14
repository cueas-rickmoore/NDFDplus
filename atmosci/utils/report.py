""" Error and perfomance reporting/logging functions.
"""
import datetime
import sys

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class SimpleLogger(object):

    def __init__(self, filepath):
        if filepath is not None:
            self.logfile = open(filepath, 'w')
        else: self.logfile = None

    def __call__(self, msg):
        if self.logfile is None: print msg
        else: self.logfile.write('%s\n' % msg)

    def close(self):
        if self.logfile is not None:
            self.logfile.close()
            self.logfile = None

    def open(self, filepath, mode='w'):
        self.close()
        self.logfile = open(filepath, mode)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class Reporter(object):

    def __init__(self, app_name, filepath=None, mode='a'):
        self.app_name = app_name

        if filepath in (None,'no'):
            self.filepath = None
            self.report_file = None
        else:
            import os
            self.filepath = os.path.normpath(filepath)
            self.report_file = open(self.filepath, mode)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def close(self):
        if self.report_file is not None:
            self.report_file.close()
            self.report_file = None
        else:
            sys.stdout.flush()

    def flush(self):
        if self.report_file is not None:
            self.report_file.flush()
        else:
            sys.stdout.flush()

    def open(self, mode='a'):
        if self.filepath is not None:
            if self.report_file is not None:
                self.report_file.close()
            self.report_file = open(self.filepath, mode)

    def timestamp(self):
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def write(self, text):
        if self.report_file is not None:
            self.report_file.write(text)
        else:
            print text

    def writeLine(self, text):
        if self.report_file is not None:
            self.report_file.write('%s\n' % text)
        else:
            print text

    def writeLines(self, lines):
        if self.report_file is not None:
            lines = ['%s\n' % line for line in lines]
            self.report_file.writelines(lines)
        else:
            for line in lines: print line

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def logError(self, message):
        self.open()
        self.reportError(message)
        self.close()

    def reportError(self, message):
        errmsg = '%s : %s : ERROR : %s'
        self.writeLine(errmsg % (self.timestamp(), self.app_name, message))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def logEvent(self, message):
        self.open()
        self.reportEvent(message)
        self.close()

    def reportEvent(self, message):
        event = '%s : %s : %s'
        self.writeLine(event % (self.timestamp(), self.app_name, message))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def logException(self, message=None, exception_type=None,
                           exception_instance=None, trace_back=None,
                           stack_limit=None):
        self.open()
        self.reportException(message, exception_type, exception_instance,
                             trace_back, stack_limit)
        self.close()

    def reportException(self, message=None, exception_type=None,
                              exception_instance=None, trace_back=None,
                              stack_limit=None):
        import traceback

        if exception_type is None:
            exception_type, exception_instance, trace_back = sys.exc_info()
        lines = traceback.format_exception(exception_type, exception_instance,
                                           trace_back, stack_limit)
        errmsg = '%s : %s : ERROR : encountered an exception ...'
        self.writeLine(errmsg % (self.timestamp(), self.app_name))

        if message is not None:
            self.writeLine(message)
        details = getattr(exception_instance, 'details', None)
        if details is not None:
            self.writeLine(details)
        self.writeLines(lines)
        self.flush()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def logInfo(self, message):
        self.open()
        self.reportInfo(message)
        self.close()

    def reportInfo(self, message):
        event = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        info = '%s : %s : INFO : %s' % (event, self.app_name, message)
        self.writeLine(info)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def logPerformance(self, elapsed_time, msg, msg_suffix=None):
        self.open()
        self.reportPerformance(elapsed_time, msg, msg_suffix)
        self.close()

    def reportPerformance(self, elapsed_time, msg, msg_suffix=None):
        now = datetime.datetime.now()
        
        if not isinstance(elapsed_time, datetime.timedelta):
            elapsed_time = now - elapsed_time
        if elapsed_time.seconds > 3600:
            hours = elapsed_time.seconds / 3600
            remainder = elapsed_time.seconds % 3600
            minutes = remainder / 60
            seconds = remainder % 60
            if hours == 1:
                perf_msg = msg + ' %d hour %d minutes %d.%06d seconds.'
            else:
                perf_msg = msg + ' %d hours %d minutes %d.%06d seconds.'
            perf_msg = perf_msg % (hours, minutes, seconds,
                                   elapsed_time.microseconds)
        elif elapsed_time.seconds > 60:
            minutes = elapsed_time.seconds / 60
            seconds = elapsed_time.seconds % 60
            perf_msg = msg + ' %d minutes %d.%06d seconds.'
            perf_msg = perf_msg % (minutes, seconds, elapsed_time.microseconds)
        else:
            perf_msg = msg + ' %d.%06d seconds.'
            perf_msg = perf_msg % (elapsed_time.seconds,
                                   elapsed_time.microseconds)

        if msg_suffix is not None:
            if not msg_suffix.startswith(' '): perf_masg += ' '
            perf_msg += msg_suffix

        info = '%s : %s : INFO : %s' % (now.strftime('%Y-%m-%d %H:%M:%S'),
                                        self.app_name, perf_msg)
        self.writeLine(info)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def reportError(app_name, message, report_filepath=None):
    reporter = Reporter(app_name, report_filepath)
    reporter.reportError(message)
    reporter.close()

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def reportEvent(app_name, message, report_filepath=None):
    reporter = Reporter(app_name, report_filepath)
    reporter.reportEvent(message)
    reporter.close()

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def reportException(app_name, message=None, exception_type=None,
                    exception_instance=None, trace_back=None,
                    stack_limit=None, report_filepath=None):
    reporter = Reporter(app_name, report_filepath)
    if message is not None:
        reporter.reportEvent(message)
    reporter.reportException(message, exception_type, exception_instance,
                             trace_back, stack_limit)
    reporter.close()

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def reportInfo(app_name, message, report_filepath=None):
    reporter = Reporter(app_name, report_filepath)
    reporter.reportInfo(message)
    reporter.close()

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def reportAppPerformance(app_name, elapsed_time, msg, msg_suffix=None,
                         report_filepath=None):
    reporter = Reporter(app_name, report_filepath)
    reporter.reportPerformance(elapsed_time, msg, msg_suffix)
    reporter.close()

def reportPerformance(elapsed_time, msg, msg_suffix=None, report_filepath=None):
    reportAppPerformance('PERF', elapsed_time, msg, msg_suffix, report_filepath)


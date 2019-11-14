
import sys
import datetime

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class ShapeMismatchException(Exception):
    pass

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def captureLastException():
    import traceback
    exception_type, exception_instance, trace_back = sys.exc_info()
    formatted = traceback.format_exception(exception_type, exception_instance,
                                           trace_back, None)
    details = getattr(exception_instance, 'details', None)
    return exception_type, formatted, details

def reportLastException(app=None, writer=None, message=None):
    exception_type, formatted, details = captureLastException()
    if app is not None:
        event = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        errmsg = '%s : %s : ERROR : encountered an exception ...' % (app, event)
    else: errmsg = 'ERROR : encountered an exception ...'
    if writer is None:
        print errmsg
        if message is not None: print message
        if details is not None: print details
        for line in formatted: print line
        sys.stdout.flush()
    else:
        writer.write('\n%s' % errmsg)
        if message is not None: writer.write('\n%s' % message)
        if details is not None: writer.write('\n%s' % details)
        writer.write('\n'.join(formatted))
        writer.flush()


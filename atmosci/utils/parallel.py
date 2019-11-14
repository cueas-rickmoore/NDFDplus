"""
Parallel processing utilities based on twisted framework
"""
import copy
import os, sys
from datetime import datetime

from twisted.internet import protocol
import twisted.internet.error

from .report import Reporter

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class ParallelProcessController(object):
    """ Starts and manages parallel processes.
    """

    def __init__(self, reactor, process_list, max_concurrent_processes=2,
                       ProtocolClass=None, debug=False,
                       reporter_or_filepath=None):

        self.reactor = reactor

        self.process_list = [ ]
        for tracking_id, executable, args, env, path in process_list:
            filename = os.path.basename(executable)
            process_args = (filename,) + (args)
            self.process_list.append( (tracking_id, executable, process_args,
                                       env, path) )

        self.max_concurrent_processes = max_concurrent_processes
        self.next_process = 0

        self.ProtocolClass = ProtocolClass
        self.debug = debug

        # create a reporter for perfomance and debug
        if isinstance(reporter_or_filepath, Reporter):
            self.reporter = reporter_or_filepath
        else:
            self.reporter = Reporter(self.__class__.__name__,
                                     reporter_or_filepath)
            self.reporter.close()

        self.failed_processes = [ ]
        self.finished_processes = [ ]
        self.running_processes = [ ]
        self.process_data = { }
        self.processes = [ None for process in self.process_list ]
        self.server_status = 'unknown'

        self.num_completed = 0

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def handleProcessEnded(self, process_index, pid, data, reason,
                                 error_reported):
        if error_reported:
            errmsg = 'child process # %d (pid = %d) failed'
            self.reporter.logError(errmsg % (process_index, pid))
            self.reporter.logError('REASON :%s' % reason.value)
        else:
            self.num_completed += 1
            msg = 'child process # %d (pid = %d) ended without errors.'
            self.reporter.logInfo(msg % (process_index, pid))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def processEnded(self, process_index, data, reason, error_reported):
        pid = self.processes[process_index][0]
        if process_index not in self.finished_processes:
            self.processes[process_index] = (pid, None)
            self.finished_processes.append(process_index)
            if error_reported:
                self.failed_processes.append((process_index,pid))
            self.process_data[pid] = data
            del self.running_processes[self.running_processes.index(pid)]

            if self.debug:
                self.reporter.logInfo('controlling %d processes' %
                                      len(self.running_processes))

            self.handleProcessEnded(process_index, pid, data, reason,
                                    error_reported)

            next_process = self._nextProcess()
            if next_process is None:
                self.handleEmptyProcessQueue()

    def processExited(self, process_index, data, reason, error_reported):
        pass

    def handleEmptyProcessQueue(self):
        if len(self.running_processes) < 1:
            self.stop()

    def start(self):
        while len(self.running_processes) < self.max_concurrent_processes:
            process = self._nextProcess()
            if process is None: break
        if self.debug:
            self.reporter.logInfo('starting reactor %s' % str(self.reactor))
        self.server_status = 'running'
        self.reactor.run()

    def stop(self):
        try:
            self.reactor.stop()
        except Exception:
            pass

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _nextProcess(self):
        next_index = self.next_process
        if next_index < len(self.process_list):
            # process_info = executable, args, env, path
            process_info = self.process_list[next_index]
            if self.debug:
                self.reporter.logInfo('starting process : %s' % process_info[0])

            if self.ProtocolClass is None:
                protocol = ParallelProcessProtocol(self, next_index,
                                                   self.reporter, self.debug)
            else:
                protocol = self.ProtocolClass(self, next_index)

            transport = self.reactor.spawnProcess(protocol, *process_info[1:])
            if self.debug:
                self.reporter.logInfo('spawned %s %s' % (process_info[0],
                                                         str(transport)))
            self.processes[next_index] = (transport.pid, transport)
            self.running_processes.append(transport.pid)

            if self.debug:
                self.reporter.logInfo('controlling %d processes' %
                                      len(self.running_processes))
            self.next_process += 1
            return transport

        # all processes have been submitted
        else:
            self.server_status = 'process queue is empty'
            return None

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class RestrictedParallelProcessController(ParallelProcessController):
    
    PROCESS_ERROR = "subprocess %d failed while processing '%s'"

    def __init__(self, reactor, process_list, max_concurrent_processes,
                       ProtocolClass, quit_time, debug=False,
                       reporter_or_filepath=None):
        ParallelProcessController.__init__(self, reactor, process_list,
                                           max_concurrent_processes,
                                           ProtocolClass, debug,
                                           reporter_or_filepath)
        self.quit_time = quit_time

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _nextProcess(self):
        next_process = ParallelProcessController._nextProcess(self)
        if next_process is None:
            return None
        if self.quit_time is not None and datetime.now() > self.quit_time:
            self.server_status = 'max run time limit was exceeded'
            return None
        return next_process

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def handleProcessEnded(self, process_index, pid, data, reason,
                                 error_reported):
        process = self.process_list[process_index][0]
        if error_reported:
            self.reporter.logError(self.PROCESS_ERROR % (pid, process))
            self.reporter.logError('REASON : %s' % str(reason.value))
            if data:
                self.reporter.logError(''.join(data))
            self.reporter.reportError(self.PROCESS_ERROR % (pid, process))
            self.reporter.flush()
        else:
            self.num_completed += 1
            msg = "subprocess %d (%s) completed."
            self.reporter.logInfo(msg % (pid, process))


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class ParallelProcessProtocol(protocol.ProcessProtocol):

    def __init__(self, controller, process_id, reporter, debug=False):
        self.controller = controller
        self.process_id = process_id
        self.reporter = reporter
        self.debug = debug

        self.data = [ ]
        self.error_reported = False

    def childDataReceived(self, childFD, data):
        if childFD == 1:
            self.outReceived(data)
        elif childFD == 2:
            self.errReceived(data)

    def childConnectionLost(self, childFD):
        if childFD == 0:
            self.inConnectionLost()
        elif childFD == 1:
            self.outConnectionLost()
        elif childFD == 2:
            self.errConnectionLost()

    def connectionMade(self):
        if self.debug:
            self.reporter.logInfo("connectionMade : %d" % self.process_id)
        # tell the process we're done sending input
        self.transport.closeStdin()

    def errConnectionLost(self):
        if self.debug:
            self.reporter.logInfo("errConnectionLost : stderr closed by %d" %
                                  self.process_id)

    def errReceived(self, data):
        if self.debug:
            self.reporter.logError("errReceived : %d " % self.process_id)
        self.reporter.logError(str(data))
        self.error_reported = True

    def inConnectionLost(self):
        if self.debug:
            self.reporter.logInfo("inConnectionLost : stdin closed to %d" %
                                  self.process_id)

    def outConnectionLost(self):
        if self.debug:
            self.reporter.logInfo("outConnectionLost : stdout closed by %d" %
                                  self.process_id)

    def outReceived(self, data):
        if self.debug:
            msg = "outReceived : data from %d : %%s" % self.process_id
            self.reporter.logInfo(msg % str(data))
        self.data.append(data)

    def processEnded(self, reason):
        if self.debug:
            self.reporter.logInfo("processEnded : %d : %s" % 
                                  (self.process_id, str(reason)))
        self.controller.processEnded(self.process_id, self.data, reason,
                                     self.error_reported)

    def processExited(self, reason):
        if self.debug:
            self.reporter.logInfo("processExited : %d : %s" % 
                                  (self.process_id, str(reason)))
        self.controller.processExited(self.process_id, self.data, reason,
                                      self.error_reported)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def discoverProcessors():
    """
    Attempt to discover the number of processors or cores on a system.
    Returns 0 (zero) if the number of processors cannot be determined.
    """
    # import multiprocessing
    # return multiprocessing.cpu_count()

    if os.access('/usr/sbin/sysctl',os.X_OK):
        command = '/usr/sbin/sysctl -n '
    elif os.access('/sbin/sysctl',os.X_OK):
        command = '/sbin/sysctl -n '
    else:
        command = None

    if command:
        # step thru the list of commonly used cpu count variables
        for variable in ('hw.physicalcpu','hw.availcpu','hw.activecpu',
                         'hw.ncpu'):
            p = subprocess.Popen(command+variable, shell=True,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 close_fds=True)
            output = p.stdout.read()
            if output:
                 output = output.strip()
                 if output.isdigt(): return int(output)

    # sysctl does not support cpu discovery
    try:
        return os.sysconf_names["SC_NPROCESSORS_ONLN"]
    except:
        pass

    return int(os.environ.get("NUMBER_OF_PROCESSORS", 0))


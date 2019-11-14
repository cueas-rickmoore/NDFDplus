
import datetime
from copy import copy

#from dateutil.parser import parse as dateParser
from dateutil.relativedelta import relativedelta
ONE_DAY = relativedelta(days=1)
import pytz

import numpy as N

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

SMALLEST_YEAR_INT = 1800
SMALLEST_DATE_INT = SMALLEST_YEAR_INT * 10000
SMALLEST_TIME_INT = SMALLEST_DATE_INT * 100

LARGEST_YEAR_INT = 2199
LARGEST_DATE_INT = (LARGEST_YEAR_INT * 10000) + 1231
LARGEST_TIME_INT = (LARGEST_DATE_INT * 100) + 23

MONTHS = ('Jan','Feb','Mar','Apr','May','Jun',
          'Jul','Aug','Sep','Oct','Nov','Dec')
MONTH_NAMES = ('January','February','March','April','May','June','July',
               'August','September','October','November','December')

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def elapsedTime(start_time, as_string=False):
    elapsed_time = datetime.datetime.now() - start_time
    if as_string:
        seconds = elapsed_time.seconds
        microseconds = elapsed_time.microseconds
        if seconds > 3600:
            hours = seconds / 3600
            remainder = seconds % 3600
            minutes = remainder / 60
            seconds = remainder % 60
            if hours == 1:
                msg = '%d hour %d mins %d.%06d secs'
            else:
                msg = '%d hours %d minutes %d.%06d seconds'
            return msg % (hours, minutes, seconds, microseconds)
        elif seconds > 60:
            minutes = seconds / 60
            seconds = seconds % 60
            msg = '%d minutes %d.%06d seconds'
            return msg % (minutes, seconds, microseconds)
        else:
            return '%d.%06d seconds.' % (seconds, microseconds)
    else: return elapsed_time

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def diffInHours(time_one, time_two, inclusive=False):
    if time_one > time_two:
        diff = time_one - time_two
    else: diff = time_two - time_one
    hours = (diff.days * 24) + (diff.seconds / 3600)
    if inclusive: return hours + 1
    return hours

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def isDaylightSavingsTime(date, timezone='US/Eastern'):
    tz = pytz.timezone(timezone)
    if isinstance(date, datetime.datetime): test_date = date
    else: test_date = asDatetime(date,True)

    indx = 0
    transition = tz._utc_transition_times[0]
    while transition < test_date:
        indx += 1
        transition = tz._utc_transition_times[indx]
    return tz._transition_info[indx][1] > datetime.timedelta(0)

def localGMT(date, timezone='US/Eastern'):
    tz = pytz.timezone(timezone)
    if isinstance(date, datetime.datetime): test_date = date
    else: test_date = datetime.datetime(*dateAsTuple(date,True))

    indx = 0
    transition = tz._utc_transition_times[0]
    while transition < test_date:
        indx += 1
        transition = tz._utc_transition_times[indx]
    return test_date + tz._transition_info[indx][0]
localUTC = localGMT

def localStandardTime(date, timezone='US/Eastern'):
    tz = pytz.timezone(timezone)
    if isinstance(date, datetime.datetime): test_date = date
    else: test_date = datetime.datetime(*dateAsTuple(date,True))

    indx = 0
    transition = tz._utc_transition_times[0]
    while transition < test_date:
        indx += 1
        transition = tz._utc_transition_times[indx]
    return test_date + tz._transition_info[indx][1]

def standardToLocalTime(date, timezone='US/Eastern'):
    tz = pytz.timezone(timezone)
    if isinstance(date, datetime.datetime): test_date = date
    else: test_date = datetime.datetime(*dateAsTuple(date,True))

    indx = 0
    transition = tz._utc_transition_times[0]
    while transition < test_date:
        indx += 1
        transition = tz._utc_transition_times[indx]
    return test_date - tz._transition_info[indx][1]

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class DateIterator(object):

    def __init__(self, start_date, end_date, julian=False):
        self.date = asDatetimeDate(start_date)
        self.end_date = asDatetimeDate(end_date)
        self.julian = julian

    def __iter__(self): return self

    def next(self):
        if self.date <= self.end_date:
            date = self.date
            self.date = date + ONE_DAY
            if self.julian: return date.timetuple().tm_yday
            else: return date
        raise StopIteration

def dateIterator(start_date, end_date, julian=False):
    return DateIterator(start_date, end_date, julian)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def daysInYear(year):
    if isLeapYear(year): return 366
    return 365

def isLeapYear(year):
    return (year % 4 == 0) and ((year % 100 != 0) or (year % 400 == 0))

def isLeapDay(date):
    if isinstance(date, (datetime.datetime, datetime.date)):
        return date.day == 29 and date.month == 2 and isLeapYear(date.year)
    elif type(date) in (tuple,list) and len(date) >= 3:
        return date[2] == 29 and date[1] == 2 and isLeapYear(date[0])
    raise ValueError, 'Invalid date : %s : %s' % (str(date),type(date))

def yearFromDate(date):
    if isinstance(date, int): return decodeIntegerDate(date)[0]
    elif isinstance(date, (tuple,list)): return date[0]
    elif isinstance(date, (datetime.datetime, datetime.date)): return date.year
    elif isinstance(date, basestring): return dateStringToTuple(date)[0]
    raise ValueError, 'Invalid date : %s : %s' % (str(date),type(date))

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def lastDayOfMonth(year_or_date, month=None):
    if isinstance(year_or_date, (datetime.date, datetime.datetime)):
        year = year_or_date.year
        month = year_or_date.month
    elif isinstance(year_or_date, int):
        year = year_or_date
    else:
        errmsg = 'First argument must be int, datetime.date, or'
        raise TypeError, errmsg + ' datetime.datetime.'

    if month in (1,3,5,7,8,10,12):
        return 31
    elif month in (4,6,9,11):
        return 30
    else:
        if isLeapYear(year): return 29
        return 28

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def decodeIntegerDate(date):
    if date > 10000000000000: # YYYYMMDDHHMMSS
        return ( date/10000000000, (date/100000000) % 100, 
                (date/1000000) % 100, (date/10000) % 100,
                (date/100) % 100, date % 100 )
    if date > 100000000000: # YYYYMMDDHHMM
        return ( date/100000000, (date/1000000) % 100, (date/10000) % 100,
                (date/100) % 100, date % 100 )
    if date > 1000000000: # YYYYMMDDHH
        return ( date/1000000, (date/10000) % 100, (date/100) % 100,
                 date % 100 )
    elif date > 10000000: # YYYYMMDD
        return ( date/10000, (date/100) % 100, date % 100 )
    elif date > 100000: #YYYYMM
        return ( date/100, date % 100, 1 )
    elif date > 999: #YYYY
        return ( date, 1, 1 )
    else:
        raise ValueError, 'Invalid integer date : %d' % date

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class DateFormatter(object):
    def __init__(self, date_format):
        self.date_format = date_format
    def __call__(self, date):
        return asDatetimeDate(date).strftime(self.date_format)
asFileDateAttr = DateFormatter('%Y-%m-%d')

def asAcisQueryDate(date):
    return asDatetimeDate(date).strftime('%Y-%m-%d')

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def asDatetimeDate(date):
    if isinstance(date, datetime.datetime):
        return datetime.date(date.year, date.month, date.day)
    elif isinstance(date, datetime.date): return date
    else: return datetime.date(*dateAsTuple(date, False))

def asDatetime(date, need_time=False):
    if isinstance(date, datetime.datetime): return date
    elif isinstance(date, datetime.date):
        return datetime.datetime(date.year, date.month, date.day, 0, 0, 0)
    else:
        _date = dateAsTuple(date, need_time)
        if need_time: return datetime.datetime(*_date)
        return datetime.datetime(_date[0], _date[1], _date[2], 0, 0, 0)

def matchDateType(date, date_obj_to_match):
    if isinstance(date_obj_to_match, datetime.datetime):
        return asDatetime(date)
    elif isinstance(date_obj_to_match, datetime.date):
        return asDatetimeDate(date)
    elif isinstance(date_obj_to_match, tuple):
        return dateAsTuple(date)
    elif isinstance(date_obj_to_match, list):
        return list(dateAsTuple(date))
    elif isinstance(date_obj_to_match, basestring):
        return dateAsString(date)
    else:
        errmsg = '"date_obj_to_match" is an unsupported type : %s'
        raise TypeError, errmsg % str(type(date_obj_to_match))

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def dateAsInt(whatever, need_hour=False, need_time=False):
    """ Converts a tuple, list or string into an integer value. Value
    is either YYYYMMDD,  YYYYMMDDHH (need_hour is True) or YYYYMMDDHHMMSS
    (need_time id True). This function will also verify that int and long
    values follow the rules for integer dates.

    NOTE: All generated dates will be validated as reasonable before
    being returned.
    """
    if isinstance(whatever, (int,long,N.int16,N.int32,N.int64)):
        if whatever > 29991231236060:
            errmsg = 'Irrational integer date value : %s'
            raise ValueError, errmsg %str(whatever)

        elif whatever > 10000000000000: # YYYYMMDDHHMMSS
            if need_time: return whatever
            if need_hour: return whatever / 10000
            else: return whatever / 1000000

        elif whatever > 100000000000: # YYYYMMDDHHMM
            if need_time: return whatever * 100
            if need_hour: return whatever / 100
            else: return whatever / 10000

        elif whatever > 1000000000: # YYYYMMDDHH
            if need_hour: return whatever
            if need_time: return whatever * 10000
            else: return whatever / 100

        elif whatever > 10000000: # YYYYMMDD
            if need_hour: return whatever * 100
            if need_time: return whatever * 1000000
            else: return whatever

        elif whatever > 100000: # YYYYMM
            if need_hour: return (whatever * 100 + 1) * 100
            if need_time: return (whatever * 100 + 1) * 10000
            else: return whatever * 100 + 1

        else:
            errmsg = 'Irrational integer date value : %s'
            raise ValueError, errmsg %str(whatever)

    # from here on, it's easier to convert eveything else to a tuple
    _whatever = dateAsTuple(whatever, need_hour or need_time)
    date = _whatever[0] * 10000 + _whatever[2] * 100 + _whatever[2]
    if need_hour or need_time: date *= 100 + _whatever[3]
    if need_time: date *= 10000 + _whatever[4] * 100 + _whatever[5]

    return date

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def dateAsString(whatever, date_format='%Y-%m-%d'):
    return asDatetime(whatever, True).strftime(date_format)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def dateAsTuple(whatever, need_time=False):
    """ Converts a tuple, list, int, long or string into a date string
    with a consistent format, either "YYYYMMDD" or "YYYYMMDDHH" when
    the hour is present.
    """
    if isinstance(whatever, (int,long))\
    or (hasattr(whatever,'dtype') and whatever.dtype.kind == 'i'):
        _whatever = decodeIntegerDate(whatever)
    elif isinstance(whatever, datetime.datetime):
        _whatever = whatever.timetuple()[:7]
    elif isinstance(whatever, datetime.date):
        _whatever = (whatever.year, whatever.month, whatever.day)
    elif isinstance(whatever, basestring):
        return dateStringToTuple(whatever, need_time)
    elif isinstance(whatever, list):
        _whatever = tuple(whatever)
    elif isinstance(whatever, tuple):
        _whatever = copy(whatever)
    else:
        errmsg = 'Unsupported date value %s : %s'
        raise ValueError, errmsg % (str(type(whatever)),str(whatever))

    # make sure the input tuple was year, month, day
    len_whatever = len(_whatever)
    if len_whatever < 3:
        errmsg = 'Input value did not contain at least year, month, day : %s'
        raise ValueError, errmsg % str(whatever)

    # sort out what is in the tuple and what was requested
    if need_time:
        if len_whatever == 3: _whatever += (0,0,0)
        elif len_whatever == 4: _whatever += (0,0)
        elif len_whatever == 5: _whatever += (0,)
        return _whatever
    return _whatever[:3]

def dateAsList(whatever, need_time=False):
    return list(dateAsTuple(whatever, need_time))

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def dateStringToTuple(whatever, need_time=False):
    """ Converts a tuple, list, int, long or string into a date string
    with a consistent format, either "YYYYMMDD" or "YYYYMMDDHH" when
    the hour is present.
    """
    if not isinstance(whatever, basestring):
        raise TypeError, 'Unsupported type for date string : %s' % type(whatever)

    if whatever.isdigit():
        string_len = len(whatever)
        if string_len >= 8:
            date = [int(whatever[:4]), int(whatever[4:6]), int(whatever[6:8])]
            if string_len >= 10: # hour
                date.append(int(whatever[8:10]))
            if string_len >= 12: # minutes
                date.append(int(whatever[10:12]))
            if string_len >= 14: # seconds
                date.append(int(whatever[12:14]))
        else:
            errmsg = 'Unable to parse date string : %s'
            raise ValueError, errmsg % whatever
    else: date = parseDateString(whatever, need_time)

    len_date = len(date)
    if need_time:
        if len_date < 4: date.extend([0,0,0])
        elif len_date < 5: date.extend([0,0])
        elif len_date < 6: date.append(0)
    else:
        if len_date > 3: date = date[:3]
    return tuple(date)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

#def parseDateString(date_str, need_time=False):
#    date = dateParser(date_str).timetuple()
#    if need_time: return date
#    else: return date[:3]

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def parseDateString(date_str, need_time=False):
    if 'T' in date_str: # date_str is an ISO string
        date_, time_ = date_str.split('T')
        if '-' in date_: parts = date_.split('-')
        elif '/' in date_: parts = date_.split('/')
        elif '.' in date_: parts = date_.split('.')
        else:
            errmsg = 'Unable to parse date string : %s'
            raise ValueError, errmsg % date_str
        if need_time: parts.extend(time_.split(':'))
    else: # date_str is NOT an ISO string#
        if '-' in date_str: parts = date_str.split('-')
        elif '/' in date_str: parts = date_str.split('/')
        elif '.' in date_str: parts = date_str.split('.')
        else:
            errmsg = 'Unable to parse date string : %s'
            raise ValueError, errmsg % date_str
        if ':' in parts[2]:
            day_and_time = [part for part in parts[2].split(':')]
            parts[2] = day_and_time[0]
            if need_time: parts.extend(day_and_time[1:])

    return [int(part) for part in parts]

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def timeSpanToIntervals(start_time, end_time, time_unit, units_per_interval=1,
                        inclusive=True):
    if time_unit in ('day','days'):
        delta = relativedelta(days=units_per_interval)
    elif time_unit in ('hour','hours'):
        delta = relativedelta(hours=units_per_interval)
    elif time_unit in ('minute','minutes'):
        delta = relativedelta(seconds=units_per_interval)
    elif time_unit in ('second','seconds'):
        delta = relativedelta(seconds=units_per_interval)
    elif time_unit in ('month','months'):
        delta = relativedelta(months=units_per_interval)
    elif time_unit in ('week','weeks'):
        delta = relativedelta(weeks=units_per_interval)
    elif time_unit in ('year','years'):
        delta = relativedelta(years=units_per_interval)

    intervals= [ ]
    if units_per_interval > 0:
        if inclusive: _end_time = end_time + delta
        else: _end_time = end_time
        _time = start_time
        while _time < _end_time:
                intervals.append(_time)
                _time += delta
    else:
        if inclusive: _end_time = end_time - delta
        else: _end_time = end_time
        _time = start_time
        while _time > _end_time:
            intervals.append(_time)
            _time += delta

    return tuple(intervals)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def yearFromDate(whatever):
    """ Converts a tuple, list, int, long or string into a date string
    with a consistent format, either "YYYYMMDD" or "YYYYMMDDHH" when
    the hour is present.
    """
    if isinstance(whatever, (int,long))\
    or (hasattr(whatever,'dtype') and whatever.dtype.kind == 'i'):
        if whatever > 1800 and whatever < 9999: return whatever
        return decodeIntegerDate(whatever)[0]
    if isinstance(whatever, datetime.date):
        return whatever.year
    if isinstance(whatever, (list,tuple)):
        return int(whatever[0])
    if isinstance(whatever, basestring):
        if len(whatever) == 4 and whatever.isdigit():
            return int(whatever)
        else: return dateStringToTuple(whatever)[0]
    errmsg = 'Unsupported date value %s : %s'
    raise ValueError, errmsg % (str(type(whatever)),str(whatever))


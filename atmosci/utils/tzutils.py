""" methods for managing time zones in datetime.datetime objects
"""
import datetime
import pytz
import tzlocal


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

ARG_TYPE_ERROR = '"%s" is an invalid type for "%s" argument.'

HOUR_STRING_FORMAT = '%Y-%m-%d:%H'
VALID_TIME_STRING = datetime.datetime(1970,1,1,0).strftime(HOUR_STRING_FORMAT)
VALID_DATE_STRING, VALID_HOUR_STRING = VALID_TIME_STRING.split(':')


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# FUNCTIONS THA OPERATE ON TIME ZONES (not times)
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def asTzinfo(timezone):
    if isinstance(timezone, pytz.tzinfo.BaseTzInfo) \
    or type(timezone) == type(pytz.UTC): return timezone
    return pytz.timezone(timezone)
asTimezoneObj = asTzinfo

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def hasValidTimezone(datetime_obj):
    return isValidTzinfo(datetime_obj.tzinfo)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def inSameTimezone(datetime_1, datetime_2):
    """
    Test whether 2 datetime.datetime instances are in the same timezone
    """ 
    return isSameTimezone(datetime_1.tzinfo, datetime_2.tzinfo)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def isInTimezone(datetime_obj, timezone):
    if isinstance(timezone, basestring):
        return isSameTimezone(datetime_obj.tzinfo, pytz.timezone(timezone))
    return isSameTimezone(datetime_obj.tzinfo, timezone)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def isSameTimezone(tzinfo_1, tzinfo_2):
    """
    Determines whether 2 instances of tzinfo are indentical. 
    """ 
    return type(tzinfo_1) == type(tzinfo_2)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def isValidTimezone(timezone):
    if isinstance(timezone, pytz.tzinfo.BaseTzInfo) \
    or type(timezone) == type(pytz.UTC): return True
    try:
        tz = pytz.timezone(timezone)
    except:
        return False
    return True

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def isValidTzinfo(tzinfo):
    return ( isinstance(tzinfo, pytz.tzinfo.BaseTzInfo) 
             or type(tzinfo) == type(pytz.UTC) )
isValidTimezoneObj = isValidTzinfo

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def timezoneAsString(timezone): return str(timezone)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def timezoneForDatetime(datetime_obj):
    if isValidTimezoneObj(datetime_obj.tzinfo): return datetime_obj.tzinfo
    return None

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def toTimeInZone(datetime_obj, timezone):
    tzinfo = asTzinfo(timezone)

    if isinstance(datetime_obj.tzinfo, pytz.tzinfo.BaseTzInfo) \
    or type(datetime_obj.tzinfo) == type(pytz.UTC):
        if isSameTimezone(datetime_obj.tzinfo, tzinfo):
            return datetime_obj
        else: return datetime_obj.astimezone(tzinfo)
    return tzinfo.localize(datetime_obj)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# FUNCTIONS THAT OPERATE ON datetime.datetime objects
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def isDst(datetime_obj):
    if datetime_obj.tzinfo is not None:
        if isinstance(datetime_obj.tzinfo, pytz.tzinfo.BaseTzInfo):
            return bool(datetime_obj.dst())
        elif type(datetime_obj.tzinfo) == type(pytz.UTC):
            return False
        errmsg = 'datetime_obj.tzinfo is not a pytz timezone.'
    else:
        errmsg = 'datetime_obj.tzinfo is None.'
    raise ValueError, errmsg

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def tzaString(datetime_obj, strftime_format='%Y-%m-%d:%H'):
    """
    Constructs a datetime string with the timezone ID appended to
    the requested datetime.strftime format

    if the datetime_obj is not timezone aware, the standard
    datetime.strftime string will be returned

    For example:
        datetime_obj = datetime.datetime(2016, 12, 25, 0, 0, 0)
        calling   tzaString(datetime_obj, '%Y-%m-%d:H')
        yields    '2016-12-25:00'

        tza_datetime = tzaDatetime(datetime_obj, 'US/Eastern')
        calling   tzaString(tza_datetime, '%Y-%m-%d:H')
        yields    '2016-12-25:00 US/Eastern'

        datetime_obj = datetime.datetime(2016, 7, 4, 20, 0, 0)
        tza_datetime = tzaDatetime(datetime_obj, 'US/Eastern')
        calling   tzaString(tza_datetime, '%Y-%m-%d:H')
        yields    '2016-07-04:20 US/Eastern DST'
    """
    time_string = datetime_obj.strftime(strftime_format)

    if isinstance(datetime_obj.tzinfo, pytz.tzinfo.BaseTzInfo):
        time_string = '%s %s' % (time_string, str(datetime_obj.tzinfo))
        try:
            if isDst(datetime_obj): return time_string + ':DST'
            else: return time_string + ':STD'
        except: # 
            pass

    elif type(datetime_obj.tzinfo) == type(pytz.UTC):
        return time_string + ' UTC'

    return time_string 

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def tzaDatetime(datetime_obj, timezone):
    """
    Converts a timezone naive datetime into a timezone aware datetime
    or converts a timzone aware datetime to the corect time in the
    requested timezone
    """
    if isValidTzinfo(timezone): tzinfo = timezone
    elif isinstance(timezone, basestring):
        try:
            tzinfo = pytz.timezone(timezone)
        except:
            errmsg = '"%s" is not a valid timezone name.'
            raise ValueError % timezone
    else:
        errmsg = '"%s" is not a valid type for the timezone argument.'
        raise TypeError % type(timezone)

    if isValidTzinfo(datetime_obj.tzinfo):
        if isSameTimezone(datetime_obj.tzinfo, tzinfo):
            return datetime_obj
        else: return datetime_obj.astimezone(tzinfo)
    else: return tzinfo.localize(datetime_obj)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# FUNCTIONS THAT OPERATE ON HOURS
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def hourFromDatetime(datetime_obj):
    return datetime_obj.replace(minute=0, second=0, microsecond=0)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def hourFromString(time_str):
    date, hour = time_str.split(':')
    year, month, day = [int(n) for n in date.split('-')]
    return datetime.datetime(year, month, day, int(hour))

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def asHourObject(time_obj):
    if isinstance(time_obj, datetime.datetime):
        return hourFromDatetime(time_obj)
    elif isinstance(time_obj, basestring):
        return hourFromString(time_obj)
    elif isinstance(time_obj, tuple):
        return datetime.datetime(*time_obj[:4])
    else:
        raise TypeError, ARG_TYPE_ERROR % (str(type(time_obj)), 'time_obj')

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def asHourInTimezone(time_obj, timezone):
    hour_obj = asHourObject(time_obj)
    if isValidTimezoneObj(hour_obj.tzinfo):
        if isValidTimezoneObj(timezone):
            return hour_obj.astimezone(timezone)
        elif isinstance(timezone, basestring):
            return hour_obj.astimezone(pytz.timezone(timezone))
    else:
        if isValidTimezoneObj(timezone):
            return timezone.localize(hour_obj)
        elif isinstance(timezone, basestring):
            return pytz.timezone(timezone).localize(hour_obj)

    raise TypeError, ARG_TYPE_ERROR % (str(type(timezone)), 'timezone')

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def hourAsString(time_obj, include_timezone=False):
    hour_str = time_obj.strftime(HOUR_STRING_FORMAT)
    if include_timezone:
        if isValidTimezoneObj(time_obj.tzinfo):
            hour_str = '%s|%s' % (hour_str, str(time_obj.tzinfo))
            hour_repr = repr(hour)
            if 'STD' in hour_repr: return '%s:STD' % hour_str
            elif 'DST' in hour_repr: return '%s:DST' % hour_str
        else:
            errmsg = '"hour" argument does not contain a valid timzone.'
            raise ValueError, errmsg
    return hour_str

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def isValidHourString(hour_str, include_timezone=False):
    if include_timezone:
        parts = hour_str.split(':')
        if len(parts) != 3: return False
        date = parts[0]
        hour = parts[1]
        #TODO: test vor valid timezone in parts[2]
    else:
        if len(time_str) != len(VALID_TIME_STRING): return False
        parts = hour_str.split(':')
        if len(parts) != 2: return False
        date = parts[0]
        hour = parts[1]

    if len(date) != len(VALID_DATE_STRING) \
    or len(hour) != len(VALID_HOUR_STRING): return False
    try:
        year, month, day = date.split('-')
    except:
        return False
    return 1900 <= int(year) <= 2100 and 1 <= int(month) <= 12 \
           and 1 <= int(day) <= 31 and 0 <= int(hour) <= 23


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# FUNCTIONS THAT OPERATE ON TIMEZONE-AWARE datetime.datetime objects
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def tzaDateString(tza_time): return tza_time.strftime('%Y%m%d')
def tzaTimeString(tza_time): return tza_time.strftime('%Y%m%d%H')
def tzaTimeStrings(tza_time, prefix='tza'):
    return { '%s_date' % prefix : tza_time.strftime('%Y%m%d'),
             '%s_hour' % prefix : tza_time.strftime('%H'),
             '%s_month' % prefix : tza_time.strftime('%Y%m'),
             '%s_time' % prefix : tza_time.strftime('%Y%m%d%H'),
             '%s_year' % prefix : str(tza_time.year) }
timeStrings = tzaTimeStrings

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def timeDiffInHours(time_obj_1, time_obj_2):
    # returns time difference in hours
    if (time_obj_1 > time_obj_2):
        time_diff = time_obj_1 - time_obj_2
    else: time_diff = time_obj_2 - time_obj_1
    days = time_diff.days 
    num_hours = (time_diff.days * 24)
    seconds = time_diff.seconds
    # if seconds < 3600, then we have whole days
    if seconds < 3600: return num_hours
    # 3600 seconds / hour ... integer math ignores remainder
    return num_hours + (seconds / 3600)
timeDifferenceInHours = timeDiffInHours

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def hoursInTimespan(time_obj_1, time_obj_2):
    return timeDiffInHours(time_obj_1, time_obj_2) + 1


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# LOCAL TIME ZONE FUNCTIONS FOR
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def asLocalTime(datetime_obj, local_timezone=None):
    if local_timezone is None: local_tzinfo = tzlocal.get_localzone()
    else: local_tzinfo = asTzinfo(local_timezone)

    if isinstance(datetime_obj.tzinfo, pytz.tzinfo.BaseTzInfo) \
    or type(datetime_obj.tzinfo) == type(pytz.UTC):
        if isSameTimezone(datetime_obj.tzinfo, local_tzinfo):
            return datetime_obj
        else: return datetime_obj.astimezone(local_tzinfo)
    return local_tzinfo.localize(datetime_obj)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def asLocalHour(datetime_obj, local_timezone=None):
    return hourFromDatetime(asLocalTime(datetime_obj, local_timezone))

def localTimezone(): return tzlocal.get_localzone()

def localTime():
    return tzlocal.get_localzone().localize(datetime.datetime.now())

def localHour():
    return hourFromDatetime(localTime())


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# UTC TIME ZONE FUNCTIONS
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def asUtcTime(datetime_obj, local_timezone=None):
    if type(datetime_obj.tzinfo) == type(pytz.UTC): return datetime_obj
    utc = pytz.timezone('UTC')

    if isinstance(datetime_obj.tzinfo, pytz.tzinfo.BaseTzInfo):
        return datetime_obj.astimezone(utc)
    else:
        if local_timezone in ('UTC', utc):
            return utc.localize(datetime_obj)
        elif local_timezone is None:
            local_tzinfo = tzlocal.get_localzone()
        else: local_tzinfo = asTzinfo(local_timezone)
        return local_tzinfo.localize(datetime_obj).astimezone(utc)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def asUTCTime(datetime_obj, local_timezone=None):
    if type(datetime_obj.tzinfo) == type(pytz.UTC):
        return datetime_obj
    utc = pytz.timezone('UTC')
    if isinstance(datetime_obj.tzinfo, pytz.tzinfo.BaseTzInfo):
        return datetime_obj.astimezone(utc)
    if local_timezone in ('UTC', utc, None):
        return utc.localize(datetime_obj)
    local_tzinfo = asTzinfo(local_timezone)
    return local_tzinfo.localize(datetime_obj).astimezone(utc)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def asUtcHour(datetime_obj, local_timezone=None):
    return hourFromDatetime(asUtcTime(datetime_obj, local_timezone))

def utcTime(): return localTime().astimezone(pytz.UTC)

def utcHour(): return hourFromDatetime(utcTime())

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def utcTimeStrings(datetime_obj, prefix='utc'):
    if type(datetime_obj.tzinfo) == type(pytz.UTC):
        return tzaTimeStrings(datetime_obj, prefix)
    elif isinstance(datetime_obj.tzinfo, pytz.tzinfo.BaseTzInfo):
        utc_time = datetime_obj.astimezone(pytz.UTC)
        return tzaTimeStrings(utc_time, prefix)
    else:
        timezone = tzlocal.get_localzone()
        utc_time = timezone.localize(datetime_obj).astimezone(pytz.UTC)
        return tzaTimeStrings(utc_time, prefix)


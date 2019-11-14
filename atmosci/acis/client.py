
import sys
import urllib, urllib2
import datetime

try:
    import simplejson as json
except ImportError:
    import json

from atmosci.utils.timeutils import asDatetimeDate

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

ALL_LOC_KEYS = ('basin','bbox','climdiv','county','cwa','sid','sids','state')
DEFAULT_ELEMS = ['maxt','mint','pcpn']
DEFAULT_URL = 'http://data.rcc-acis.org/'
NONE_ERR = '"elems" may not be None. A valid element specification is required.'

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def tightJson(value): return json.dumps(value, separators=(',', ':'))

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class AcisWebServicesClient(object):

    def __init__(self, base_url=DEFAULT_URL, valid_elems=DEFAULT_ELEMS,
                       loc_keys=ALL_LOC_KEYS, date_required=True, 
                       verbose=False, debug=False):
        self.base_url = base_url
        self.date_required = date_required
        self.debug = debug
        self.prev_query = None
        self.loc_keys = loc_keys
        self.valid_elems = valid_elems
        self.verbose = verbose
        #!TODO : list of valid metadata and validation method 

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def request(self, query_type, **request_dict):
        raise NotImplementedError

    def submitRequest(self, query_type, **request_dict):
        debug = self.debug
        if debug:
            print 'submitRequest', query_type
            print request_dict
        query_json = self.jsonFromRequest(query_type, request_dict)
        return self.submitQuery(query_type, query_json)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def query(self, json_string):
        raise NotImplementedError

    def submitQuery(self, query_type, json_string):
        ERROR_MSG = 'Error processing response to query : %s %s'
        debug = self.debug
        verbose = self.verbose or self.debug

        if debug:
            print '\n', self.__class__.__name__, 'submitQuery', query_type
            print json_string
        if not isinstance(json_string, basestring):
            raise TypeError, '"json_string" argument must be a string'
        try:
            json_query_dict = json.loads(json_string)
        except:
            errmsg = '"json_string" argument is not a valid JSON string :\n%s'
            raise ValueError, errmsg % json_string
            
        url = self.base_url
        if url.endswith('/'):
            if query_type.startswith('/'): url += query_type[1:]
            else: url += query_type
        else:
            if query_type.startswith('/'): url += query_type
            else: url += '/' + query_type

        if verbose:
            print 'POST', url
            print 'params =', json_string

        post_params = urllib.urlencode({'params':json_string})
        if debug: print '\nencoded json string\n', post_params
        req = urllib2.Request(url, post_params,
                              { 'Accept':'application/json' }
                             )
        if debug:
            print 'request', req.get_full_url(), req.get_data()
        url += ' json=' + post_params
        try:
            response = urllib2.urlopen(req)
        except Exception as e:
            setattr(e, 'details', ERROR_MSG % ('POST',url))
            raise e

        try:
            response_string = response.read()
        except Exception as e:
            setattr(e, 'details', ERROR_MSG % ('POST',url))
            raise e
        if debug: print 'response', response_string

        # track last successful query
        self.prev_query = json_string

        return response_string, response

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def acisDateString(self, date):
        return asDatetimeDate(date).strftime('%Y-%m-%d')

    def acisStringToDate(self, acis_date_str):
        return datetime.date(*[int(part) for part in acis_date_str.split('-')])

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def jsonFromRequest(self, query_type, request_dict):
        # look for a valid date or date range
        if self.date_required:
            query_dict = self._validateRequestDate(request_dict)
        else: query_dict = { }

        # make sure there is a valid location for this type of request
        key, location = self._validateLocation(request_dict)
        query_dict[key] = location

        # metadata
        metadata = self._validateMetadata(request_dict)
        if metadata is not None: query_dict['meta'] = metadata

        # data elements
        if query_type.endswith('Data'):
            query_dict['elems'] = self._validateDataElems(request_dict)

        # query type specific updates
        required = self._classSpecificRequirements(request_dict)
        if required is not None:
            query_dict.update(required)

        return json.dumps(query_dict)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def jsonToPython(self, response_string, response):
        """ Convert a json string to Python objects ... handle known instances
        where server injects badly formed JSON into the stream
        """
        if 'DOCTYPE HTML PUBLIC' in response_string:
            errmsg = 'SERVER ERROR : '
            if 'server encountered an internal error' in response_string:
                errmsg += 'server encountered an unspecified internal error.'
                ecode = 503
            else:
                ecode = 500
                errmsg += 'server returned HTML, not valid JSON.\n'
                errmsg += response_string
            raise urllib2.HTTPError(response.geturl(),ecode,errmsg,None,None)

        server_error = 'SERVER ERROR : '
        errors = [ ]
        if '[Failure instance:' in response_string:
            found_start = response_string.find('[Failure instance:')
            while found_start > 0:
                found_end = response_string.find('\n],',found_start)
                error = response_string[found_start:found_end+3]
                errors.append(''.join(error.splitlines()))
                before = response_string[:found_start]
                after = response_string[found_end+3:]
                response_string = before + after
                found_start = response_string.find('[Failure instance:')
        if errors:
            errmsg = 'the following errors found in returned JSON string :'
            print server_error, errmsg
            for error in errors:
                print error
            print 'The resulting data block may be incomplete.'
            sys.stdout.flush()

        try:
           return json.loads(response_string)
        except Exception as e:
            errmsg += 'unable to handle improperly formated JSON from server.\n'
            errmsg += response.geturl() + '\n'
            errmsg += 'Returned JSON = ' + response_string
            setattr(e, 'details', errmsg)
            raise e

    # - # - # - # - # - # - # - # - # - # - # - # - # - # - # - # - # - # - #

    def _classSpecificRequirements(self, request_dict):
        return None

    def _validateDataElems(self, request_dict):
        if 'elems' not in request_dict:
            raise KeyError, '"elems" key missing from request.'

        elems = request_dict['elems']
        if elems is None:
            raise ValueError, NONE_ERR
        elif isinstance(elems, basestring):
            if elems.startswith('['): return json.dumps(elems)
            elif elems.startswith('{'): return [json.dumps(elems),]
            elif ',' in elems:
                return [ {"name":name} for name in elems.split(',') ]
            else: return [ {"name":elems} ]
        elif isinstance(elems, (list,tuple)):
            return [ {"name":name} for name in elems ]
        elif isinstance(elems, dict): return elems
        else:
            raise TypeError, TYPE_ERR % type(elems)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _validateRequestDate(self, request_dict):
        if 'date' in request_dict:
            return { 'date' : self.acisDateString(request_dict['date']) }
        elif 'sdate' in request_dict and 'edate' in request_dict:
            return { 'sdate' : self.acisDateString(request_dict['sdate']),
                     'edate' : self.acisDateString(request_dict['edate']) }
        else: 
            raise KeyError, 'Request has no date or an incomplete date span.'

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _validateLocation(self, request_dict):
        # make sure there is a valid location in the request
        #if query_type == 'StnMeta':
        #    loc_keys = ['state','county','climdiv','cwa','basin','bbox']
        #if query_type == 'General':
        #    loc_keys = ['state','county','climdiv','cwa','basin']

        union = list(set(self.loc_keys) & set(request_dict))
        if len(union) == 0:
            raise KeyError, "No location key in %s query." % query_type
        elif len(union) > 1:
            raise KeyError, "Multiple location keys in %s query." % query_type
        key = union[0]
        return key, request_dict[key]

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _validateMetadata(self, request_dict):
        return request_dict.get('meta', request_dict.get('metadata', None))



from atmosci.acis.client import AcisWebServicesClient

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from atmosci.acis.client import DEFAULT_URL
STN_LOC_KEYS = ('sid',)
MULTI_LOC_KEYS = ('basin','bbox','climdiv','county','cwa','sids','state')
VALID_ELEMS = ['maxt','mint','pcpn','snow','snwd',
               'cdd','cddNN','hdd','hddNN','gdd','gddNN']

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class AcisStationDataClient(AcisWebServicesClient):

    def __init__(self, base_url=DEFAULT_URL, valid_elems=VALID_ELEMS,
                       debug=False):
        super(AcisStationDataClient, self).\
        __init__(base_url=base_url, valid_elems=valid_elems,
                 loc_keys=STN_LOC_KEYS, date_required=True, debug=debug)
        #!TODO : list of valid metadata

    def request(self, **request_dict):
        return self.submitRequest( 'StnData', **request_dict)

    def query(self, json_query_string):
        return self.submitQuery('StnData', json_query_string)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class AcisMultiStationDataClient(AcisWebServicesClient):

    def __init__(self, base_url=DEFAULT_URL, valid_elems=VALID_ELEMS,
                       debug=False):
        super(AcisMultiStationDataClient, self).\
        __init__(base_url=base_url, valid_elems=valid_elems,
                 loc_keys=MULTI_LOC_KEYS, date_required=True, debug=debug)
        #!TODO : list of valid metadata
    
    def request(self, **request_dict):
        return self.submitRequest( 'MultiStnData', **request_dict)

    def query(self, json_query_string):
        return self.submitQuery('MultiStnData', json_query_string)


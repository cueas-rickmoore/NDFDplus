
from atmosci.acis.client import AcisWebServicesClient

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class AcisGeneralDataClient(AcisWebServicesClient):

    def __init__(self, base_url=DEFAULT_URL, valid_elems=VALID_ELEMS,
                       debug=False):
        super(AcisGeneralDataClient, self).\
        __init__(base_url=base_url, valid_elems=None,
                 loc_keys=('state','bbox','id'), date_required=False,
                 debug=debug)
        #!TODO : list of valid metadata

    def request(self, area, **request_dict):
        if area in ('state','county','climdiv','cwa','basin'):
            query_type = 'General/%s' % area
        else:
            raise ValueError, 'Invalid area type : %s' % area
        return self.submitRequest(query_type, **request_dict)

    def query(self, area, json_query_string):
        if area in ('state','county','climdiv','cwa','basin'):
            query_type = 'General/%s' % area
        else:
            raise ValueError, 'Invalid area type : %s' % area
        return self.submitQuery(query_type, json_query_string)


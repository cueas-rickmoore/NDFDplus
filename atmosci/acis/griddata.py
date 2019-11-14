
import datetime

import numpy as N

from atmosci.acis.client import AcisWebServicesClient, json
from atmosci.acis.gridinfo import acisGridNumber

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from atmosci.acis.client import DEFAULT_URL
VALID_ELEMS = ['maxt','mint','pcpn','cdd','cddNN','hdd','hddNN','gdd','gddNN']

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class AcisGridDataClient(AcisWebServicesClient):

    def __init__(self, base_url=DEFAULT_URL, valid_elems=VALID_ELEMS,
                       verbose=False, debug=False):
        super(AcisGridDataClient, self).\
        __init__(base_url=base_url, valid_elems=valid_elems,
                 loc_keys=('bbox','loc','state'), date_required=True,
                 verbose=verbose, debug=debug)
        #!TODO " list of valid metadata

    def request(self, grid_id, **request_dict):
        grid_num = acisGridNumber(grid_id)
        return self.submitRequest('GridData', grid=grid_num, **request_dict)

    def query(self, json_query_string):
        return self.submitQuery('GridData', json_query_string)

    def _classSpecificRequirements(self, request_dict):
        return { 'grid':request_dict['grid'] }

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class AcisGridDownloadMixin:

    def getAcisGridData(self, acis_grid_id, elems, start_date, end_date=None,
                              include_dates=True, **kwargs):
        debug = kwargs.get('debug',False)
        verbose = kwargs.get('verbose',debug)
        if 'bbox' in kwargs:
            bbox = kwargs['bbox']
            if isinstance(bbox, tuple):
                bbox = list(bbox)
            if isinstance(bbox, list):
                bbox = str(bbox)[1:-1]
            bbox = bbox.replace(' ','')
            request = { "bbox":"%s" % bbox, }
        elif 'point' in kwargs:
            point = kwargs['point']
            if isinstance(point, tuple):
                bbox = list(point)
            if isinstance(point, list):
                point = str(point)[1:-1]
            point = point.replace(' ','')
            request = { "point":"%s" % point, }
        elif 'state' in kwargs:
            request = { "state":kwargs['state'], }
        else:
            errmsg = 'No area bounds criteria specified.'
            errmsg += ' One of "point", "bbox" or "state" is required.'
            raise KeyError, errmsg

        client = AcisGridDataClient(debug=debug)
        _start_date = client.acisDateString(start_date)
        if end_date is not None:
            _end_date = client.acisDateString(end_date)
            request["sdate"] = _start_date
            request["edate"] = _end_date
        else: request["date"] = _start_date

        meta = kwargs.get('meta', None)
        if isinstance(meta, basestring):
            request["meta"] = meta
        elif isinstance(meta, (list,tuple)):
            request["meta"] = ','.join(meta)

        request['elems'] = elems

        if verbose: print 'getAcisGridData :\n', request

        # returns python dict { 'meta' = { "lat" : grid, 'lon' : grid }
        #                       'data' = [ [date string, grid] ]
        query_result = json.loads(client.request(acis_grid_id, **request)[0])

        data_dict = \
        self.unpackAcisQueryResults(query_result, elems, meta, include_dates)
        return data_dict

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def acisStringToDate(self, acis_date_str):
        return datetime.date(*[int(part) for part in acis_date_str.split('-')])

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def unpackAcisQueryResults(self, query_result, elems, meta=False,
                                     include_dates=True):
        # unpack the grids
        if isinstance(elems, basestring): _elems = elems.split(',')
        else: _elems = elems
        data = [ ]
        for result in query_result['data']:
            date_result = [self.acisStringToDate(result[0]),]
            for indx, grid in enumerate(result[1:]):
                elem_name = _elems[indx]
                date_result.append(self.unpackAcisGrid(elem_name, grid))
            data.append(date_result)

        if len(data) > 1:
            # convert list of (date, elem, ...) tuples to date tuple,
            # and a data tuple for each element
            data = zip(*data)
            # convert data tuples to arrays
            for indx, grid in enumerate(data[1:],start=1):
                data[indx] = N.array(grid)
        else:
            date = data[0][0]
            data = [grid for grid in data[0][1:]]
            data.insert(0,(date,))

        if include_dates:
            data_dict = { 'dates': data[0], }
        else: data_dict = { }
        for indx, elem_name in enumerate(_elems, start=1):
            data_dict[elem_name] = data[indx]

        if meta is not None:
            meta_dict = query_result['meta']
            if 'elev' in meta_dict:
                meta_dict['elev'] = self.unpackAcisGrid('elev', meta_dict['elev'])
            if 'lat' in meta_dict:
                meta_dict['lat'] = self.unpackAcisGrid('lat', meta_dict['lat'])
                meta_dict['lon'] = self.unpackAcisGrid('lon', meta_dict['lon'])
            data_dict.update(meta_dict)

        return data_dict

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def unpackAcisGrid(self, elem, grid):
        narray = N.array(grid)
        narray = narray.astype(float)
        # missing = N.where(narray < -998)
        #narray[missing] = N.nan
        return narray


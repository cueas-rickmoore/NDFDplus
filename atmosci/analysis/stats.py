
from scipy.stats import stats as scipy_stats
import numpy as N

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

COUNTS_RECORD_TYPE = ( ('min','<i2'), ('max','<i2'), ('mean','<i2'),
                       ('median','<i2'), ('stddev','<i2'), ('coverage','<i4'),
                       ('processed','<i2',6), )
EMPTY_COUNTS_RECORD = (-32768, -32768, -32768, -32768, -32768, -32768, (0,0,0,0,0,0))

STATS_RECORD_TYPE = ( ('min','f4'), ('max','f4'), ('mean','f4'),
                      ('median','f4'), ('stddev','f4'), ('coverage','<i4'),
                      ('processed','<i2',6), )
EMPTY_STATS_RECORD = (N.nan, N.nan, N.nan, N.nan, N.nan, -32768, (0,0,0,0,0,0))

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def arrayStatistics(numpy_array, missing_value=N.nan):
    if N.isfinite(missing_value):
        valid_values = numpy_array[N.where(numpy_array!=missing_value)]
        if numpy_array.dtype.kind == 'f':
            valid_values = valid_values[N.where(N.isfinite(valid_values))]
    else:
        valid_values = numpy_array[N.where(N.isfinite(numpy_array))]

    if len(valid_values) > 0:
        statistics =  { 'min' : N.min(valid_values),
                        'max' : N.max(valid_values),
                        'mean' : N.mean(valid_values),
                        'stddev' : N.std(valid_values),
                        'median' : N.median(valid_values),
                        'mode' : scipy_stats.mode(valid_values),
                        'missing' : len(numpy_array) - len(valid_values),
                      }
    else:
        statistics =  { 'min' : missing_value, 'max' : missing_value,
                        'mean' : missing_value, 'stddev' : 0.0,
                        'median' : missing_value,
                        'mode' : ( N.array([missing_value,]),
                                   N.array([len(numpy_array),]) ),
                        'missing' : len(numpy_array),
                      }
    return statistics

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def emptyStatsDataset(num_records, descrip_field=None):
    if descrip_field is None:
        empty_record = EMPTY_STATS_RECORD
        record_type = STATS_RECORD_TYPE
    else:
        empty_record = list(EMPTY_STATS_RECORD)
        empty_record.insert(0,'')
        record_type = list(STATS_RECORD_TYPE)
        record_type.insert(0, (descrip_field[0],'|S%s' % str(descrip_field[1])))
    records = [empty_record for record in range(num_records)]
    return N.rec.fromrecords(records, record_type, (num_records,))

def initStatsDataset(data_manager, dataset_name, num_records, attrs=None):
    if not data_manager.datasetExists(dataset_name):
        dataset = emptyStatsDataset(num_records)
        return data_manager.createDataset(dataset_name, dataset, attrs)
    return None

def newStatsRecord(process_date, stats, descrip_field=()):
    if isinstance(process_date, basestring):
        proc_date, proc_time = process_date.split()
        _process_date_ = tuple( [int(num) for num in proc_date.split('-')] +
                                [int(num) for num in proc_time.split(':')] )
    elif isinstance(process_date, datetime.datetime):
        _process_date_ = (process_date.year, process_date.month,
                          process_date.day, process_date.hour,
                          process_date.minute, process_date.second)
    elif isinstance(process_date, datetime.date):
        _process_date_ = (process_date.year, process_date.month,
                          process_date.day, 12, 0, 0)

    stat_rec = [ stats.get('min',N.nan), stats.get('max',N.nan),
                 stats.get('mean',N.nan), stats.get('median',N.nan),
                 stats.get('stddev',N.nan), stats.get('coverage',-32768),
                 _process_date_ ]
    if descrip_field:
        record_type = list(STATS_RECORD_TYPE)
        record_type.insert(0, (descrip_field[0],'|S%s' % str(descrip_field[1])))
        stat_rec.insert(0, stats.get(descrip_field[0],''))
    else:
        record_type = STATS_RECORD_TYPE
    return N.rec.fromrecords([stat_rec,], record_type, (1,))[0]


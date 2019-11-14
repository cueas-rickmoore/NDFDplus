
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

POSTAL_NCDC_MAP = { 'AL':'01','AK':'50','AZ':'02','AR':'03',
                    'CA':'04','CO':'05','CT':'06',
                    'DE':'07','FL':'08','GA':'09','HI':'51',
                    'ID':'10','IL':'11','IN':'12', 'IA':'13',
                    'KS':'14','KY':'15','LA':'16',
                    'ME':'17','MD':'18', 'MA':'19','MI':'20',
                    'MN':'21','MS':'22','MO':'23','MT':'24',
                    'NE':'25','NV':'26','NH':'27','NJ':'28',
                    'NM':'29','NY':'30', 'NC':'31','ND':'32',
                    'OH':'33','OK':'34','OR':'35',
                    'PA':'36', 'PR':'66','PI':'91',
                    'RI':'37','SC':'38','SD':'39',
                    'TN':'40','TZ':'41','UT':'42',
                    'VT':'43','VA':'44', 'VI':'67',
                    'WA':'45','WV':'46','WI':'47','WY':'48' }

NCDC_POSTAL_MAP = { }
for postal, ncdc in POSTAL_NCDC_MAP.items():
    NCDC_POSTAL_MAP[ncdc] = postal
del ncdc, postal

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def postal2ncdc(postal):
    """ given US postal code, returns NCDC area code """
    return POSTAL_NCDC_MAP.get(postal.upper(),None)

def ncdc2postal(nrcc):
    """ given NCDC area code, returns US postal code """
    return NCDC_POSTAL_MAP.get(str(nrcc), None)


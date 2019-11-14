def runningMean(x, N):
	y = numpy.zeros((len(x),))
	for ctr in range(len(x)):
		y[ctr] = numpy.sum(x[ctr:(ctr+N)])
	return y/N


def alexandersson_test(diff_series):

### Conducts alexandersson's inhomogeneity test given a list of sorted regression residuals 

## COMPUTE DIFFERENCE (BASE -REFERENCE) SERIES

	brkpnt = {}
	intervals = [0,len(diff_series)]

#	while len(intervals)> 1:
	short_diff = []
	if intervals[1]-intervals[0] < 11:
		del intervals[0]   #### series too short to test
#		continue

	for yr in range (intervals[0],intervals[1]):
		short_diff.append(diff_series[yr])

	norm_diff = stats.zscore(short_diff)

	diff_sd = numpy.std(short_diff)

	tmax = -999.
	for chpt in range (24,len(norm_diff)-24):  ### look at one day (24 hour blocks)
	
		list_1 = []
		list_2 = []

###  COMPUTE TEST VALUES FOR SUBSERIES FROR FIRST PART OF THE SERIES
		
		if chpt%1000==0:print intervals[0], chpt, intervals[1]
		for yr in range(len(norm_diff)):
			if yr <= chpt:
				list_1.append(norm_diff[yr])
			else:
				list_2.append(norm_diff[yr])

		avg_1 = numpy.average(list_1)
		avg_2 = numpy.average(list_2)

		t = len(list_1)*avg_1**2 + len(list_2)*avg_2**2

		tmax = max(tmax,t)

		if t == tmax:
			bpoint = intervals[0]+chpt+1
			offset = (avg_2-avg_1)*diff_sd
#				print offset,avg_2,avg_1,diff_sd

	test = 1
	tcrit = 1.3192*math.log((len(list_1)+len(list_2)))+3.1421 #95% eqn from Khaliq and Ouarda (2006) IJC fit for n = 10 to 150 in "SNHT critical values from KO2006.xlsx"  PAULA's CORRECTION TO ABOVE
#           print len(list_1)+len(list_2), tcrit
            
	if tmax > tcrit:test = 0

	if test == 1:
 		print 'HOMOGENEOUS',intervals
		del intervals[0]
	else:
		print 'INHOMOGENEOUS', bpoint,intervals
		if len(brkpnt.keys()) > 1:
			if bpoint <> brkpnt.keys()[len(brkpnt.keys())-1]:
				brkpnt[bpoint]=[offset,tmax,tcrit]
				intervals.insert(1,bpoint)
			else:
				brkpnt[bpoint]=[offset,tmax,tcrit]
				intervals.insert(1,bpoint)

	if len(brkpnt.keys())>1:
		list_1 = []
		list_2 = []
		brk_list = brkpnt.keys()
		brk_list.sort()
		brk_list.insert(0,0)
		for brks in range(len(brk_list)-2):
			list_1 = []
			list_2 = []
			for yr in range (brk_list[brks],brk_list[brks+1]):
				list_1.append(diff_series[yr])
			for yr in range (brk_list[brks+1],brk_list[brks+2]):
				list_2.append(diff_series[yr])
			avg_1 = arts_routines.avger(list_1)
			avg_2 = arts_routines.avger(list_2)
			brkpnt[brk_list[brks+1]][0]=avg_2-avg_1

	return brkpnt



import numpy

import scipy
from scipy import stats

import math

input_file = 'Test_data.txt'
#input_file = 'dewpt_depr_for_33686_vs_33619.txt'

ifile = open(input_file,'r')

x_val = []
y_val = []
while 1:
	line = ifile.readline()
	if len(line)==0:break
	
	newline =  line.strip()
	newline = newline.split()
	
	if input_file == 'Test_data.txt':
		x_val.append(newline[0])
		y_val.append(newline[1])
	elif input_file == 'dewpt_depr_for_33686_vs_33619.txt':
		if newline[1]<>'inf' and newline[2]<>'inf':
			x_val.append(newline[1])
			y_val.append(newline[2])
		
x_val= numpy.vstack([x_val, numpy.ones(len(x_val))]).T
	
#x_val = numpy.array(x_val)
y_val = numpy.array(y_val)

regres = numpy.linalg.lstsq(x_val, y_val)

m, c = regres[0]
resid_array = []
day_max_array =[]

for val in range(len(x_val)):
	resid_array.append(float(y_val[val])-float(x_val[val][0])*m+c)
	if val%24==0 and val<>0:
		day_max_array.append(max(resid_array[-24:]))
	
#run_resid = runningMean(resid_array,24)

#day_max_array.sort()

first_diff = abs(numpy.array(day_max_array[0:len(day_max_array)-1])-numpy.array(day_max_array[1:]))


breakpt = alexandersson_test(day_max_array)

	


import numpy as N

def movingAverage(span, narray):
    nsize = len(narray)
    maxi = nsize - 1
    avg_array = N.empty(nsize)
    i = 0
    while i < nsize:
        if i > span:
            j = i - span
        else:
            j = 0
        if i < maxi:
            k = i + span
        else:
            k = -1
        avg_array[i] = N.mean(narray[j:k])
        i+=1
    return avg_array

def movingSum(span, narray):
    nsize = len(narray)
    maxi = nsize - 1
    sum_array = N.empty(nsize)
    i = 0
    while i < nsize:
        if i > span:
            j = i - span
        else:
            j = 0
        if i < maxi:
            k = i + span
        else:
            k = -1
        sum_array[i] = N.sum(narray[j:k])
        i+=1
    return sum_array


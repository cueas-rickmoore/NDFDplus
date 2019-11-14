

#-----------------------------------------------------------
# Send in a list of three consecutive elements and a blip
# minimum threshold (must be equalled or exceeded to flag)
# and returns 1 if flagged, 0 otherwise (except if Nones
# are present in the list, in which case None is returned).
#
def isBlip(listOfThree,blipThresh):
  from gijs_stuff2 import sign
  if listOfThree.count(None)==0:
    d1=listOfThree[1]-listOfThree[0]
    d2=listOfThree[2]-listOfThree[1]
    ad1=abs(d1)
    ad2=abs(d2)
    shortLeg=min(ad1,ad2)
    diffSign=(sign(d1)!=sign(d2) and sign(d1)!=0)
    if shortLeg>=blipThresh and diffSign==1:
      return 1
    else:
      return 0
  else:
    return None


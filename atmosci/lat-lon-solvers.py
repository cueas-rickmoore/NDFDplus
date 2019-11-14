

def lonkey(lon):
	spacing = 0.041667
	diff = 82.75 + lon # subtract from abs value of west-most Longitude
	factor = int(round(diff / spacing))
	key = -82.75 + (factor * spacing)
	return round(key, 3)


def latkey(lat):
	spacing = 0.041667
	diff = 47.708333 - lat # subtract from north-most Latitde
	factor = int(round(diff / spacing))
	key = 47.708333 - (factor * spacing)
	return round(key, 3)

from ingestion.models import WifiScan
import math
import time
import os
import pandas as pd
import numpy as np
from scipy.misc import imresize
from matplotlib import cm
from PIL import Image, ImageDraw
from django.db import connection
from django.conf import settings

ZOOM_OFFSET = {
    12: 1,
    13: .5,
    14: .2,
    15: .11,
    16: .06,
    17: .03,
    18: .02
}


def getTopSSIDs(threshold=settings.SSID_THRESHOLD):
    cursor = connection.cursor()
    cursor.execute('SELECT ssid FROM wifi_scan GROUP BY ssid HAVING COUNT(*) > %s;' % (threshold))
    return [i[0] for i in cursor.fetchall() if i[0]]

def getBoundingBox(ssid):
    cursor = connection.cursor()
    cursor.execute('SELECT MIN(lat), MAX(lat), MIN(lng), MAX(lng) from wifi_scan WHERE ssid="%s" AND acc < 50' % (ssid))
    r = cursor.fetchall()[0]
    return {
        'nw_corner': [r[1], r[2]],
        'se_corner': [r[0], r[3]]
    }
    
def getGreyBoundingBox():
    cursor = connection.cursor()
    cursor.execute('SELECT FORMAT(MIN(lat),4), FORMAT(MAX(lat),4), FORMAT(MIN(lng),4), FORMAT(MAX(lng),4) from wifi_scan WHERE lat>0')
    r = cursor.fetchall()[0]
    return {
        'nw_corner': [r[1], r[2]],
        'se_corner': [r[0], r[3]]
    }

def getPath(ssid, agg_function, zoom, x, y):
    path = os.path.join(
	settings.TILE_DIR, ssid, agg_function,
	str(zoom), str(x), '%s.png' % y
    )
    return path
    
def getGreyPath(zoom, x, y):
    path = os.path.join(
	settings.GREYTILE_DIR, 
	str(zoom), str(x), '%s.png' % y
    )
    return path

def generateTiles(ssid):
    zoom_range=range(settings.ZOOM_MIN, settings.ZOOM_MAX+1)
    boundingBox = getBoundingBox(ssid)

    df = pd.DataFrame.from_records(
	WifiScan.objects.filter(ssid=ssid).values('lat', 'lng', 'level')
    ).round(4)
	
    for zoom in zoom_range:
        nw_corner = deg2num(
            boundingBox['nw_corner'][0] + ZOOM_OFFSET[zoom],
            boundingBox['nw_corner'][1] - ZOOM_OFFSET[zoom],
            zoom
        )

        se_corner = deg2num(
            boundingBox['se_corner'][0] - ZOOM_OFFSET[zoom],
            boundingBox['se_corner'][1] + ZOOM_OFFSET[zoom],
            zoom
        )

        for x in range(nw_corner[0], se_corner[0]+1):
            for y in range(nw_corner[1], se_corner[1]+1):
                for agg_function in settings.AGGREGATION:
                    tile = generateTile(x, y, zoom, {
                        'ssid': ssid,
                        'agg_function': agg_function
                    }, df)

                    path = getPath(ssid, agg_function, zoom, x, y)
		    if not os.path.exists(os.path.dirname(path)):
		        os.makedirs(os.path.dirname(path))

                    tile.save(path)

def generateGreyTiles():
    zoom_range=range(settings.ZOOM_MIN, settings.ZOOM_MAX+1)
    boundingBox = getGreyBoundingBox()
    
    df = pd.DataFrame.from_records(
	WifiScan.objects.values('lat', 'lng')
    ).round(4)
    for zoom in zoom_range:
        nw_corner = deg2num(
            float(boundingBox['nw_corner'][0]) + float(ZOOM_OFFSET[zoom]),
            float(boundingBox['nw_corner'][1]) - float(ZOOM_OFFSET[zoom]),
            zoom
        )

        se_corner = deg2num(
            float(boundingBox['se_corner'][0]) - float(ZOOM_OFFSET[zoom]),
            float(boundingBox['se_corner'][1]) + float(ZOOM_OFFSET[zoom]),
            zoom
        )

        for x in range(nw_corner[0], se_corner[0]+1):
            for y in range(nw_corner[1], se_corner[1]+1):
                tile = generateGreyTile(x, y, zoom, df)

                path = getGreyPath(zoom, x, y)
		if not os.path.exists(os.path.dirname(path)):
		   os.makedirs(os.path.dirname(path))

                tile.save(path)


def num2deg(xtile, ytile, zoom):
    n = 2.0 ** zoom
    lon_deg = xtile / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ytile / n)))
    lat_deg = math.degrees(lat_rad)
    return (lat_deg, lon_deg)

def deg2num(lat_deg, lon_deg, zoom):
  lat_rad = math.radians(lat_deg)
  n = 2.0 ** zoom
  xtile = int((lon_deg + 180.0) / 360.0 * n)
  ytile = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
  return (xtile, ytile)

def generateTile(x, y, zoom, params, allRecords=None):
    timestamp = int(time.time())
    ssid = params['ssid']
    agg_function = params['agg_function']

    nw_corner = num2deg(x, y, zoom)
    se_corner = num2deg(x+1, y+1, zoom)

    lats = [min(nw_corner[0], se_corner[0]), max(nw_corner[0], se_corner[0])]
    lngs = [min(nw_corner[1], se_corner[1]), max(nw_corner[1], se_corner[1])]

    lats2 = np.around([lats[0] - .0001, lats[1] + .0001], decimals=4)
    lngs2 = np.around([lngs[0] - .0001, lngs[1] + .0001], decimals=4)

    timestamp = int(time.time())

    if allRecords is not None:
        df = allRecords[
            (allRecords.lat >= lats2[0]) &
            (allRecords.lat <= lats2[1]) &
            (allRecords.lng >= lngs2[0]) &
            (allRecords.lng <= lngs2[1])
        ]
             
    else:
        records = WifiScan.objects.filter(
            ssid=ssid,
            lat__gte=lats2[0], lat__lte=lats2[1],
            lng__gte=lngs2[0], lng__lte=lngs2[1],
        ).values('lat', 'lng', 'level')


        timestamp = int(time.time())

        df = pd.DataFrame.from_records(
            records
        )

        timestamp = int(time.time())

        df = df.round(4)


    timestamp = int(time.time())


    if len(df) == 0:
        return Image.new("RGBA", (256,256))

    groups = df.groupby(('lat', 'lng'), as_index=False)
    points = getattr(groups, agg_function)()


    size = np.rint([(lngs2[1] - lngs2[0]) / .0001 + 1, (lats2[1] - lats2[0]) / .0001 + 1])

    zi, xi, yi = np.histogram2d(
        points['lng'], points['lat'], weights=points['level'],
        bins=size, normed=False, range=[lngs2, lats2]
    )

    zi = np.ma.masked_equal(zi, 0)
    zi = ((np.clip(zi, -90, -29) + 91) * 4.25).astype(int)

    pixels = imresize(np.rot90(zi), size=(256,256), interp='nearest') / 255.0

    color = np.uint8(cm.jet(pixels) * 255)

    color[pixels == 0,3] = 0

    timestamp = int(time.time())

    return Image.fromarray(color)

def generateGreyTile(x, y, zoom, params, allRecords=None):
    timestamp = int(time.time())
    nw_corner = num2deg(x, y, zoom)
    se_corner = num2deg(x+1, y+1, zoom)

    lats = [min(nw_corner[0], se_corner[0]), max(nw_corner[0], se_corner[0])]
    lngs = [min(nw_corner[1], se_corner[1]), max(nw_corner[1], se_corner[1])]

    lats2 = np.around([lats[0] - .0001, lats[1] + .0001], decimals=4)
    lngs2 = np.around([lngs[0] - .0001, lngs[1] + .0001], decimals=4)

    timestamp = int(time.time())

    if allRecords is not None:
        df = allRecords[
            (allRecords.lat >= lats2[0]) &
            (allRecords.lat <= lats2[1]) &
            (allRecords.lng >= lngs2[0]) &
            (allRecords.lng <= lngs2[1])
        ]
             
    else:
        records = WifiScan.objects.values('lat', 'lng')

        timestamp = int(time.time())

        df = pd.DataFrame.from_records(
            records
        )

        timestamp = int(time.time())

        df = df.round(4)


    timestamp = int(time.time())


    if len(df) == 0:
        return Image.new("RGBA", (256,256))
    print df.head()
    greyLayer = ImageDraw.Draw.point(df,'grey')

    timestamp = int(time.time())

    return greyLayer

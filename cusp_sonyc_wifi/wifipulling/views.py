from django.shortcuts import render

# Create your views here.
from django.http import HttpResponse
from ingestion.models import WifiScan
import simplejson as json
import datetime
import time
from django.db import connection
import math
import pandas as pd
import numpy as np
from scipy.misc import imresize
from matplotlib import cm
from PIL import Image

col_name = {'idx':1, 'lat':1, 'lng':1, 'acc':1, 'altitude':1, 'time':1, 'device_mac':1, 'app_version':1, 'droid_version':1, 'device_model':1, 'ssid':1, 'bssid':1, 'caps':1, 'level':1, 'freq':1}


def num2deg(xtile, ytile, zoom):
    n = 2.0 ** zoom
    lon_deg = xtile / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ytile / n)))
    lat_deg = math.degrees(lat_rad)
    return (lat_deg, lon_deg)

def generateTile(x, y, zoom, request):
    ssid = request.GET.get('ssid', None)
    agg_function = request.GET.get('agg_function', 'median')

    nw_corner = num2deg(x, y, zoom)
    se_corner = num2deg(x+1, y+1, zoom)

    lats = [min(nw_corner[0], se_corner[0]), max(nw_corner[0], se_corner[0])]
    lngs = [min(nw_corner[1], se_corner[1]), max(nw_corner[1], se_corner[1])]

    lats2 = np.around([lats[0] - .0001, lats[1] + .0001], decimals=4)
    lngs2 = np.around([lngs[0] - .0001, lngs[1] + .0001], decimals=4)

    df = pd.DataFrame.from_records(
        WifiScan.objects.filter(
            ssid=ssid,
            lat__gte=lats2[0], lat__lte=lats2[1],
            lng__gte=lngs2[0], lng__lte=lngs2[1],
        ).values('lat', 'lng', 'level')
    ).round(4)

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

    return Image.fromarray(color)

def tile(request, zoom, x, y):
    response = HttpResponse(content_type="image/png")
    generateTile(int(x), int(y), int(zoom), request).save(response, "PNG")
    return response


def index(request):

    batch = 10 #default
    offset = 0 #default
    b_size = request.GET.get('batch', '')
    off_size = request.GET.get('offset', '')
    if (b_size != ''):
        try:
            batch = int(b_size)
        except:
            pass
    if (off_size != ''):
        try:
            offset = int(off_size)
        except:
            pass
    idx_start = offset
    idx_end = offset + batch
    is_full_size = True if (b_size==''and off_size=='') else False

    q_idx = request.GET.get('idx', '')
    q_lat = request.GET.get('lat', '')
    q_lng = request.GET.get('lng', '')
    q_radius = request.GET.get('radius', '')
    q_acc = request.GET.get('acc', '')
    q_alt = request.GET.get('altitude', '')
    q_startdate = request.GET.get('startdate', '')
    q_enddate = request.GET.get('enddate', '')
    q_dev_mac = request.GET.get('device_mac', '')
    q_app_v = request.GET.get('app_version', '')
    q_dro_v = request.GET.get('droid_version', '')
    q_dev_m = request.GET.get('device_model', '')
    q_ssid = request.GET.get('ssid', '')
    q_bssid = request.GET.get('bssid', '')
    q_caps = request.GET.get('caps', '')
    q_lvl = request.GET.get('level', '')
    q_frq = request.GET.get('freq', '')
    q_colname = request.GET.get('columns', '')
    q_decimal = request.GET.get('decimal', '')

    response_data = []
    tem=[]
    if (q_decimal != ''):
        decimal_place = 8

        try:
            decimal_place = int(q_decimal)
        except:
            pass

        cursor = connection.cursor()
        cursor.execute('SELECT DISTINCT TRUNCATE(lat,%d),TRUNCATE(lng,%d) FROM wifi_scan' % (decimal_place, decimal_place))
        tem = cursor.fetchall()

    else:
        query_set = None
        try:
            query_set = WifiScan.objects.all()
        except:
            pass

        if (query_set != None):
            if (q_idx != ''): # int
                try:
                    query_set = query_set.filter(idx=q_idx)
                except:
                    pass
            if (q_acc != ''): #float
                try:
                    query_set = query_set.filter(acc__gte=q_acc)
                except:
                    pass
            if (q_alt != ''): #double
                try:
                    query_set = query_set.filter(altitude__gte=q_alt)
                except:
                    pass
            if (q_startdate != ''):
                try:
                    mth, day, year = q_startdate.split('/',2)
                    dt = datetime.date(int(year), int(mth), int(day))
                    t_stamp = time.mktime(dt.timetuple()) * 1000
                    query_set = query_set.filter(time__gte=t_stamp)
                except:
                    pass
            if (q_enddate != ''):
                try:
                    mth, day, year = q_enddate.split('/',2)
                    dt = datetime.date(int(year), int(mth), int(day))
                    t_stamp = time.mktime(dt.timetuple()) * 1000
                    query_set = query_set.filter(time__lt=t_stamp)
                except:
                    pass
            if (q_dev_mac != ''):
                query_set = query_set.filter(device_mac=q_dev_mac)
            if(q_app_v != ''):
                query_set = query_set.filter(app_version=q_app_v)
            if (q_dro_v != ''):
                query_set = query_set.filter(droid_version=q_dro_v)
            if (q_dev_m != ''):
                query_set = query_set.filter(device_model=q_dev_m)
            if (q_ssid != ''):
                try:
                    list_ssid = q_ssid.split('|')
                    multi_ssid = ''
                    for id in list_ssid:
                        if (id != ''):
                            multi_ssid += "ssid="+"\'" + id + "\'" + " OR "
                    query_set = query_set.extra(where=[multi_ssid[:-4]])
                except:
                    pass
            if (q_bssid != ''):
                query_set = query_set.filter(bssid=q_bssid)
            if(q_caps != ''):
                query_set = query_set.filter(caps__contains=q_caps)
            if (q_lvl != ''):
                try:
                    query_set = query_set.filter(level__gte=q_lvl)
                except:
                    pass
            if (q_frq != ''):
                try:
                    query_set = query_set.filter(freq=q_frq)
                except:
                    pass

            #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
            human_readable = 0
            q_timeformat = request.GET.get('timeformat', '')
            try:
                human_readable = int(q_timeformat)
            except:
                pass

            is_distinct = 0
            q_distinct = request.GET.get('distinct', '')
            try:
                is_distinct = int(q_distinct)
            except:
                pass

            list_name=[]
            if (q_colname == ''):
                if (is_distinct == 1):
                    tem=query_set.values().distinct()
                else:
                    tem=query_set.values()
            else:
                list_name = q_colname.split('|')
                args = []
                for name in list_name:
                    if name in col_name:
                        args.append(name)
                if (is_distinct == 1):
                    tem=query_set.values(*args).distinct()
                else:
                    tem=query_set.values(*args)

            #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
            if (is_full_size == False):
                tem = tem[idx_start:idx_end]

            #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
            key = 'time'
            if (q_colname == '' or key in list_name):
                if (human_readable == 1):
                    for item in tem:
                        item[key]=(datetime.datetime.fromtimestamp(item[key]/1000)).strftime('%m-%d-%Y %H:%M:%S')
                elif(human_readable == 2):
                    for item in tem:
                        item['time2']=(datetime.datetime.fromtimestamp(item[key]/1000)).strftime('%m-%d-%Y %H:%M:%S')

    response_data = list(tem)

    return HttpResponse(json.dumps(response_data), content_type="application/json")

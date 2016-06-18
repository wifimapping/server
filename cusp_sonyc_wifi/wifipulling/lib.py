from django.db import connection
import math
import pandas as pd
import numpy as np
from django.conf import settings

settings.configure()
cursor = connection.cursor()
cursor.execute('SELECT DISTINCT caps FROM wifi_scan WHERE ssid=\'nyu\'')
data = cursor.fetchone()
print 'So far so good'

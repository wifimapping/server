from django.db import models

class UniqueLocations(models.Model):
    idx = models.AutoField(primary_key=True)
    lat = models.DecimalField(max_digits=8, decimal_places=4)
    lng = models.DecimalField(max_digits=8, decimal_places=4)

    class Meta:
        unique_together = ('lat', 'lng')
        managed = True
        db_table = 'unique_locations'

    def __unicode__(self):
        return " / " + str(self.lat) + " / " + str(self.lng)

class TempUniqueLocations(models.Model):
    idx = models.AutoField(primary_key=True)
    lat = models.DecimalField(max_digits=8, decimal_places=4)
    lng = models.DecimalField(max_digits=8, decimal_places=4)

    class Meta:
        managed = True
        db_table = 'temp_unique_locations'

    def __unicode__(self):
        return " / " + str(self.lat) + " / " + str(self.lng)


class WifiScan(models.Model):
    idx = models.BigIntegerField(primary_key=True)
    lat = models.FloatField()
    lng = models.FloatField()
    acc = models.FloatField()
    altitude = models.FloatField()
    time = models.DecimalField(max_digits=15, decimal_places=0)
    device_mac = models.CharField(max_length=20)
    app_version = models.CharField(max_length=10)
    droid_version = models.CharField(max_length=10)
    device_model = models.CharField(max_length=50)
    ssid = models.CharField(max_length=100)
    bssid = models.CharField(max_length=20)
    caps = models.CharField(max_length=100)
    level = models.FloatField()
    freq = models.FloatField()

    class Meta:
        managed = False
        db_table = 'wifi_scan'

    def __unicode__(self):
        return str(self.idx) + " / " + str(self.lat) + " / " + str(self.lng) + " / " + str(self.acc) + " / " + str(self.altitude) + " / " + str(self.time) + " / " + self.device_mac + " / " + self.app_version+ " / " + self.droid_version+ " / " + self.device_model+ " / " + self.ssid+ " / " + self.bssid+ " / " + self.caps+ " / " + str(self.level)+ " / " + str(self.freq)

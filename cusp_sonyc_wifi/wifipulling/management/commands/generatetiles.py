from django.core.management.base import BaseCommand
from wifipulling.lib import getTopSSIDs, generateTiles

class Command(BaseCommand):
    help = 'Generates and saves tiles for the top SSIDs'

    def handle(self, *args, **options):
        ssids = ['Rounter 70'] # getTopSSIDs()
        for ssid in ssids:
            generateTiles(ssid)

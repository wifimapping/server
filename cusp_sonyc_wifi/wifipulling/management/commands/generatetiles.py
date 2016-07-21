from django.core.management.base import BaseCommand
from wifipulling.lib import getTopSSIDs, generateTiles

class Command(BaseCommand):
    help = 'Generates and saves tiles for the top SSIDs'

    def add_arguments(self, parser):
        parser.add_argument('ssids', nargs='*', type=str)

    def handle(self, *args, **options):
        if options['ssids']:
            ssids = options['ssids']
        else:
            ssids =  getTopSSIDs()
            ssids.append('Known Open Wi-Fi')

        for ssid in ssids:
            generateTiles(ssid)

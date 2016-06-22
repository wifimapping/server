from django.core.management.base import BaseCommand
from wifipulling.lib import generateGrayTiles

class Command(BaseCommand):
    help = 'Generates and saves tiles for all the locations'
    
generateGrayTiles()

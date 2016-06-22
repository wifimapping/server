from django.conf.urls import patterns, url
from wifipulling import views

urlpatterns = patterns('',
    url(r'^$', views.index, name='index'),
    url(r'tile/(\d+)/(\d+)/(\d+)/', views.tile, name='tile')
    url(r'graytile/(\d+)/(\d+)/(\d+)', views.gray)
)

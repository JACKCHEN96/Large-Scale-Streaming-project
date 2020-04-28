"""stream_phase0 URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.conf.urls import url
from django.contrib import admin
from django.urls import path

from . import views

urlpatterns = [
    # path('admin/', admin.site.urls),
    url(r'hello/', views.hello_world),
    url(r'admin/', admin.site.urls),
    url(r'index/', views.index, name="index"),
    url(r'workload_generator', views.workload_generator,
        name="workload_generator"),
    url(r'page1/', views.page1_view),
    url(r'page2/', views.page2_view),
    url(r'show_info', views.show_info),
    url(r'^data_gen_test$', views.data_gen_test),
    url(r'^data_gen_test_get_res$', views.data_gen_test_get_res),
    url(r'^data_gen_start', views.data_gen_start),
    url(r'^data_gen_stop', views.data_gen_stop),
    # custom templates
    path(r"filters/template0", views.custom_template0, name="template0"),
    path(r"filters/template1", views.custom_template1, name="template1"),
    # data source
    path(r"data/template0", views.data_template0),
    path(r"data/template1", views.data_template1),
    url(r'', views.homepage, name="homepage"),
]

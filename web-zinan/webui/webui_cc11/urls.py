from django.urls import path
from . import views

urlpatterns = [
    path('webui_cc11/', views.members, name='members'),
    path('webui_cc11/process/', views.process_query, name='process_query'),
]
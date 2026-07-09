from django.urls import path
from .views import views
from django.contrib.auth.views import LogoutView

urlpatterns = [
    # Home page URLs
    path('home/', views.home_view, name='home'),
    path('home/gbt_events/', views.gbt_event_partial, name='gbt_events'),
    path('home/dsoc_events/', views.dsoc_event_partial, name='dsoc_events'),
    path('submit-waveform/', views.submit_waveform, name='submit_waveform'),

    # Dashboard page URLs
    path('dashboard/', views.dashboard_view, name='dashboard_home'),
    path('dashboard/update/', views.event_table_partial, name='event_table_update'),
    path('dashboard/graph', views.latency_graphing, name='latency_graphing'),
    path('dashboard/image/<int:event_id>/', views.serve_image, name ='serve_image'),

    

    # add logout path 
    path('logout/', LogoutView.as_view(next_page='login'), name='logout'),
]

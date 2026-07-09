from django.urls import path
from .views import views

urlpatterns = [
    # Have not tested if this works as expected
    path('index/', views.live_dashboard, name='index'),

    path('dashboard/', views.dashboard_view, name='dashboard_home'),
    
    # Have not tested if this works as expected
    path('dashboard/update/', views.event_table_partial, name='event_table_update'),

    path('dashboard/graph', views.latency_graphing, name='latency_graphing'),

    # visiting this path for the image 
    path('dashboard/image/<int:event_id>/', views.serve_image, name ='serve_image'),

    path('submit-waveform/', views.submit_waveform, name='submit_waveform'),

    path('new_observation/',views.create_observation, name='create_new_observation')
]

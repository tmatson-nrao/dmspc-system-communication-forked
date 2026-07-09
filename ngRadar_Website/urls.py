from django.urls import path
from .views import views
from django.contrib.auth.views import LogoutView

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

    # Keep as placeholder when we develop this
    # # path to blank page where we will allow new observations to be created
    # path('new_observation/',views.create_observation, name='create_new_observation'),
    
    # add logout path 
    path('logout/', LogoutView.as_view(next_page='login'), name='logout')
]

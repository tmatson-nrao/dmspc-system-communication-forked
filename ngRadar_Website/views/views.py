from django.shortcuts import redirect, render
from django.contrib.auth.decorators import login_required

#libraries to get files from the outside directory
import sys
from pathlib import Path

#libraries used for data streaming
import json
from django.http import StreamingHttpResponse

from ngRadar_Website.models.models import ObservatoryEvent
from django.contrib import messages
from django.contrib.auth import authenticate, login
from django.db.models import Avg

def login_view(request):
    if request.method == 'POST':
        username_input = request.POST['username']
        password_input = request.POST['password']
        
        # This automatically uses the Argon2 settings to verify the password string
        user = authenticate(request, username=username_input, password=password_input)
        
        if user is not None:
            login(request, user)
            return redirect('dashboard_home')
        else:
            messages.error(request, "Invalid username or password.")
            return render(request, 'registration/login.html')
            
    return render(request, 'registration/login.html')


#import the producer
outside_dir = str(Path(__file__).resolve().parents[2])
sys.path.append(outside_dir)

def get_dashboard_context():
    """Helper function to keep data uniform across view updates"""
    latest_events = ObservatoryEvent.objects.all().order_by('-created_at')[:20]
    
    # Calculate the average latency of the last 20 records in Postgres
    avg_latency = latest_events.aggregate(Avg('latency_ms'))['latency_ms__avg'] or 0.0

    # Calculate anything else we need for the initial load of the dashboard
    
    return {
        'events': latest_events,
        'avg_latency': round(avg_latency, 2)
    }

@login_required
def dashboard_view(request):
    # this is the initial view to load the dashboard
    context = get_dashboard_context()

    return render(request, 'ngRadar_Website/dashboard.html', context) # pass any other vars to frontend here

def event_table_partial(request):
    # this is the partial template view for updating the observatory events table
    context = get_dashboard_context()

    return render(request, 'ngRadar_Website/partials/dashboard_updates.html', context)

# Need a function AND another partial template for handling the user inputted payload
# Don't worry about this until Sprint 45 I think
# def user_input_partial(request):
#     # this is the partial template view for handling user input
#     # can we store user inputted data to the database here? should we?
#     # how should we handle sending the user inputted payload to the Kafka topic?
#     return render(request, 'user_input.html') # just an example .html, have not actually created this



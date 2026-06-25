#libraries to get files from the outside directory
import sys, os
from pathlib import Path

#libraries used for data streaming
import json
from django.http import StreamingHttpResponse

from django.shortcuts import render

script_dir = os.path.dirname(os.path.realpath(__file__))
absolute_path = os.path.abspath(os.path.join(script_dir, '../models'))
sys.path.append(absolute_path)

from models import ObservatoryEvent
from enums import Stations # if needed? if not, pls delete

#import the producer
outside_dir = str(Path(__file__).resolve().parents[2])
sys.path.append(outside_dir)
from mock_producer import run_mock_producer


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

def dashboard_view(request):
    # this is the initial view to load the dashboard
    context = get_dashboard_context()

    return render(request, 'dashboard.html', context) # pass any other vars to frontend here

def event_table_partial(request):
    # this is the partial template view for updating the observatory events table
    context = get_dashboard_context()

    return render(request, 'dashboard_updates.html', context) # pass any other vars to frontend here

# Need a function AND another partial template for handling the user inputted payload
def user_input_partial(request):
    # this is the partial template view for handling user input
    # can we store user inputted data to the database here? should we?
    # how should we handle sending the user inputted payload to the Kafka topic?
    return render(request, 'user_input.html') # just an example .html, have not actually created this


# I feel like we should have a robust login view here to validate user credentials before allowing access to the dashboard.
def login_view(request):
    # Handle user login logic here
    pass # placeholder for login logic

from django.shortcuts import redirect, render
from ngRadar_Website.models.models import ObservatoryEvent
from ngRadar_website.models.models import Users
from django.contrib import messages
from enum import Stations # if needed? if not, pls delete



def login_view(request):
    if request.method == 'POST':
        username = request.POST['username']
        password = request.POST['password']
        user = Users.objects.filter(username=username, password=password).first()
        if user:
            # Login successful
            return render(request, user, 'dashboard')
        else:
            # Login failed
            messages.error(request, "Invalid username or password.")
            return redirect('login') 


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
# Don't worry about this until Sprint 45 I think
def user_input_partial(request):
    # this is the partial template view for handling user input
    # can we store user inputted data to the database here? should we?
    # how should we handle sending the user inputted payload to the Kafka topic?
    return render(request, 'user_input.html') # just an example .html, have not actually created this



from django.shortcuts import redirect, render
from django.contrib.auth.decorators import login_required

#libraries to get files from the outside directory
import sys
from pathlib import Path

#libraries used for data streaming
import json
from django.http import StreamingHttpResponse, Http404, HttpResponse

from ngRadar_Website.models.models import ObservatoryEvent, uiEvent, gbtEvent, dsocEvent
from django.contrib import messages
from django.contrib.auth import authenticate, login
from django.db.models import Avg
from confluent_kafka import Producer
import os 
import uuid
from datetime import datetime, timezone 
from confluent_kafka import Producer
from dotenv import load_dotenv
load_dotenv(override=True)

#program constants
DATE_TIME_STRING=19

def login_view(request):
    if request.method == 'POST':
        username_input = request.POST['username']
        password_input = request.POST['password']
        
        # This automatically uses the Argon2 settings to verify the password string
        user = authenticate(request, username=username_input, password=password_input)
        
        if user is not None:
            login(request, user)
            return redirect('home')
        else:
            messages.error(request, "Invalid username or password.")
            return render(request, 'registration/login.html')
            
    return render(request, 'registration/login.html')




def update_observatory_events_table(new_events):
    # This function updates the ObservatoryEvent table with the latest events from both gbtEvent and dsocEvent tables
    for event in new_events:
        # Check if the event already exists in the ObservatoryEvent table
        if not ObservatoryEvent.objects.filter(gbt_uuid=event.gbt_uuid, event_time=event.event_time).exists():
            # Create a new ObservatoryEvent entry
            ObservatoryEvent.objects.create(
                object_id=event.object_id,
                gbt_uuid=event.uuid if isinstance(event, gbtEvent) else None,
                target=event.target,
                tx_waveform=event.tx_waveform,
                rec_waveform=event.rec_waveform,
                event_time=event.event_time,
                latency_ms=event.latency_ms,
                xmit_station=getattr(event, 'xmit_station', None),
                rcvr_station=getattr(event, 'rcvr_station', None),
                image_url=getattr(event, 'image_url', None),
                num_bytes=getattr(event, 'num_bytes', None)
            )
    return ObservatoryEvent.objects.all().order_by('-event_time')


def get_dashboard_context():
    """Helper function to keep data uniform across view updates"""

    # Grab latest events from GBT + DSOC tables, and update the ObservatoryEvent table with the latest events from both tables
    gbt_events = gbtEvent.objects.all().order_by('-event_time')
    dsoc_events = dsocEvent.objects.all().order_by('-event_time')
    new_events = list(gbt_events) + list(dsoc_events)
    latest_events = update_observatory_events_table(new_events)
    
    # Calculate the average latency of the last 20 records in Postgres
    avg_latency = latest_events.aggregate(Avg('latency_ms'))['latency_ms__avg'] or 0.0

    
    return {
        'events': latest_events,
        'avg_latency': round(avg_latency, 2)
    }



def home_view(request):
    # this is the view for the homepage, which will display the most recent observatory event
    latest_events = get_dashboard_context()['events']
    most_recent_event = latest_events.first() if latest_events else None
    context = {
        'latest_event': most_recent_event
    }
    return render(request, 'ngRadar_Website/home.html', context)

def get_Message_Latency():
    last_message_latency_str = str(ObservatoryEvent.objects.last().latency_ms)
    last_message_time_str = str(ObservatoryEvent.objects.last().event_time)
    last_message_time_str = last_message_time_str[:DATE_TIME_STRING]  # Truncate to first 20 characters
    
    data_to_send = {
        "latency": last_message_latency_str,
        "time_sent": last_message_time_str
    }
    yield f"data: {json.dumps(data_to_send)}\n\n"

def latency_graphing(request):
    response = StreamingHttpResponse(
        get_Message_Latency(),
        content_type="text/event-stream; charset=utf-8"
    )
    response["Cache-Control"] = "no-cache"
    return response


def dashboard_view(request):
    context = get_dashboard_context()
    return render(request, 'ngRadar_Website/dashboard.html', context) # pass any other vars to frontend here


def event_table_partial(request):
    # this is the partial template view for updating the observatory events table
    context = get_dashboard_context()

    return render(request, 'ngRadar_Website/partials/dashboard_updates.html', context)


def serve_image(request, event_id):

    # Fetch image file path from ObservatoryEvent table and query the file from the SeaweedFS server
    # To render image in the browser, when clicked on the image in the observatory events table
    obs_event_image = ObservatoryEvent.objects.get(id=event_id)
    image = get_file_from_seaweedfs(obs_event_image.image_file)
    # if no image found, show 404
    if not image: 
        raise Http404
    
    # send raw bytes to browser, labeled as PNG 
    return HttpResponse(image, content_type ='image/png')



def get_file_from_seaweedfs(image_path):
    import requests
    # Construct the full URL to the SeaweedFS server
    seaweedfs_url = f"{os.environ['WEED_S3_DOMAIN']}/{image_path}"

    try:
        response = requests.get(seaweedfs_url)
        response.raise_for_status()  # Raise an error for bad responses (4xx or 5xx)
        return response.content  # Return the binary content of the image
    except requests.RequestException as e:
        print(f"Error fetching image from SeaweedFS: {e}")
        raise Http404("Image not found.")
    

# Need a function AND another partial template for handling the user inputted payload
def submit_waveform(request):
    if request.method == "POST":
        uuid_input = uuid.uuid4()
        waveform  = request.POST.get('waveform')
        timestamp = datetime.now()
        # Database version
        ui_Event = uiEvent.objects.create(
            uuid = uuid_input,
            selected_waveform = waveform,
            event_time = timestamp
        )

        
        # Kafka version 
        topic = "user_input"
        config = {
            "bootstrap.servers": os.environ["BOOTSTRAP_SERVER"],
            "message.max.bytes": 8388608,
            "client.id": "ui-producer"}
        message = "User input a new waveform."

        def produce(topic, config, key, value):
            producer = Producer(config)
            producer.produce(topic, key=key, value=value)
            
            producer.flush()

        def main():
            key = uuid_input.hex  # Use the UUID as the key for the Kafka message
            value = json.dumps(message).encode("utf-8")
            produce(topic, config, key, value)
        main()
        
    return redirect('index')

def dsoc_event_partial(request):
    # this is the partial template view for updating the dsoc events table
    dsoc_events = dsocEvent.objects.all().order_by('-event_time')
    context = {
        'dsoc_events': dsoc_events
    }
    return render(request, 'ngRadar_Website/partials/dsoc_home_partial.html', context)

def gbt_event_partial(request):
    # this is the partial template view for updating the gbt events table
    gbt_events = gbtEvent.objects.all().order_by('-event_time')
    context = {
        'gbt_events': gbt_events
    }
    return render(request, 'ngRadar_Website/partials/gbt_home_partial.html', context)
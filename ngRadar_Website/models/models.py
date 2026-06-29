#libraries to get files from the outside directory
import sys
from pathlib import Path
from django.db import models
from ngRadar_Website.enums import Stations



class ObservatoryEvent(models.Model):
    # Mapping to your payload variables
    object_id = models.CharField(max_length=100)
    target = models.CharField(max_length=100)
    tx_waveform = models.CharField(max_length=100, blank=True, null=True)
    rec_waveform = models.CharField(max_length=100, blank=True, null=True)

    product_type = models.CharField(max_length=50, blank=True, null=True)
    product_id = models.CharField(max_length=100, blank=True, null=True)
    station = models.PositiveSmallIntegerField(
            choices=Stations.choices,
            default=Stations.GBT, blank=True, null=True
        )    
    event_time = models.DateTimeField()
    created_at = models.DateTimeField(blank=True, null=True) 

    # This allows us to track the transmitter and receiver stations for each event
    xmit_station = models.CharField(
        max_length=100, 
        choices=Stations.choices, 
        blank=True, null=True
    )
    rcvr_station = models.CharField(
        max_length=100, 
        choices=Stations.choices, 
        blank=True, null=True
    )
    
    image_file = models.ImageField(upload_to='ddm_payloads/', blank=True, null=True)
    num_bytes = models.IntegerField(blank=True, null=True)
    latency_ms = models.FloatField(default=0.0)


    def __str__(self):
        return f"Obs: {self.object_id} | {self.xmit_station} -> {self.rcvr_station}"


    # what other tables may we need?
    
# what other tables may we need?

import time
from datetime import datetime, timedelta
import pyModeS as pms
from pyModeS.extra.tcpclient import TcpClient
import geopy.distance
import paho.mqtt.client as mqtt
import json
from numpy import arctan2,random,sin,cos,degrees
import numpy
import math

class ADSBClient(TcpClient):
    def __init__(self, host, port, rawtype):
        self.PTDB = {}
        self.msgCache = {}
        self.qth = [51.973357353305914,5.669655084220917]
        self.localAirspace = [] 
        self.mqtt = mqtt.Client()
        super(ADSBClient, self).__init__(host, port, rawtype)

    def calculate_initial_compass_bearing(self, pointA, pointB):
        if (type(pointA) != tuple) or (type(pointB) != tuple):
            raise TypeError("Only tuples are supported as arguments")
        lat1 = math.radians(pointA[0])
        lat2 = math.radians(pointB[0])
        diffLong = math.radians(pointB[1] - pointA[1])
        x = math.sin(diffLong) * math.cos(lat2)
        y = math.cos(lat1) * math.sin(lat2) - (math.sin(lat1)
                * math.cos(lat2) * math.cos(diffLong))
        initial_bearing = math.atan2(x, y)
        initial_bearing = math.degrees(initial_bearing)
        compass_bearing = (initial_bearing + 360) % 360
        return compass_bearing

    def handle_planet_entry(self, icao):
        self.mqtt.connect("10.208.11.32",1883)
        self.mqtt.publish("space/planes/geozone/enter", json.dumps(self.PTDB[icao]))
        self.mqtt.disconnect()
        print(self.PTDB[icao]['callsign'], "Entered our airspace", json.dumps(self.PTDB[icao]))

    def handle_planet_exit(self, icao):
        self.mqtt.connect("10.208.11.32",1883)
        self.mqtt.publish("space/planes/geozone/exit", json.dumps(self.PTDB[icao]))
        self.mqtt.disconnect()
        print(self.PTDB[icao]['callsign'], "Exited our airspace")

    def handle_cleanup(self, ts):
        self.PTDB = {k:v for k,v in self.PTDB.items() if v['timestamp'] > (time.time() - 180)}
        self.msgCache = {k:v for k,v in self.msgCache.items() if v['timestamp'] > (ts - timedelta(minutes=3))}

    def handle_modes(self, msg):
        icao = pms.adsb.icao(msg)
        tc = pms.adsb.typecode(msg)
        bds = pms.bds.infer(msg)
        if bds == "BDS20":
            self.PTDB[icao]['callsign'] = pms.commb.cs20(msg).strip("_")


    def handle_adsb(self, msg):
        icao = pms.adsb.icao(msg)
        tc = pms.adsb.typecode(msg)

        if icao not in self.PTDB:
            self.PTDB[icao] = {
                    "callsign": "",
                    "alt": 0,
                    "lat":0.0,
                    "lon":0.0,
                    "distance":0,
                    "timestamp":0,
                    "airspace": 0,
                    "entered":0,
                    "distance":0,
                    "speed":0,
                    "heading":0,
                    }
        if icao not in self.msgCache:
            self.msgCache[icao] = {
                    "msg_even": 0,
                    "msg_odd":0,
                    "t_even":0,
                    "t_odd":0,
                    }

        self.PTDB[icao]['timestamp'] = time.time() 
        self.msgCache[icao]['timestamp'] = datetime.now()

        # Typecode 1-4 Aircraft identification and category
        if tc>=1 and tc<=4:
            self.PTDB[icao]['callsign'] = pms.adsb.callsign(msg).strip("_")
            self.PTDB[icao]['category'] = pms.adsb.category(msg)

        if (tc==19):
            speed_heading = pms.adsb.speed_heading(msg)
            self.PTDB[icao]['speed'] = speed_heading[0]
            self.PTDB[icao]['heading'] = speed_heading[1] 

        # Typecode 5-8 (surface), 9-18 (airborne, barometric height), and 20-22 (airborne, GNSS height)
        if (tc>=5 and tc<=8) or (tc>=9 and tc<=18) or (tc>=20 and tc<=22):
            self.PTDB[icao]['alt'] = pms.adsb.altitude(msg)
            if not pms.decoder.adsb.oe_flag(msg):
                self.msgCache[icao]['msg_even'] = msg 
                self.msgCache[icao]['t_even'] = time.time() 
            else:
                self.msgCache[icao]['msg_odd'] = msg 
                self.msgCache[icao]['t_odd'] = time.time()

            if (self.msgCache[icao]['msg_odd'] and self.msgCache[icao]['msg_even']):
                pos = pms.adsb.position(
                        self.msgCache[icao]['msg_even'],
                        self.msgCache[icao]['msg_odd'],
                        self.msgCache[icao]['t_even'],
                        self.msgCache[icao]['t_odd'], self.qth[0], self.qth[1])
                if not pos or not ((-90 < pos[0] < 90) and (-180 < pos[1] < 180)):
                    return
                self.PTDB[icao]['lat'] = pos[0]
                self.PTDB[icao]['lon'] = pos[1]
                self.PTDB[icao]['distance'] = geopy.distance.geodesic(self.qth, pos).km
                self.PTDB[icao]['bearing'] = self.calculate_initial_compass_bearing(tuple(self.qth), tuple(pos))
            if (self.PTDB[icao]['distance'] < 50 
                    #and self.PTDB[icao]['alt'] < 200000
                    and self.PTDB[icao]['callsign'] != ""
                    and self.PTDB[icao]['lat'] != 0
                    and self.PTDB[icao]['lon'] != 0
                    ):
                if icao not in self.localAirspace:
                    self.PTDB[icao]['airspace'] = 1
                    self.PTDB[icao]['entered'] = time.time()
                    self.localAirspace.append(icao)
                    self.handle_planet_entry(icao)
            else:
                if icao in self.localAirspace:
                    self.PTDB[icao]['airspace'] = 0
                    self.PTDB[icao]['entered'] = 0
                    self.handle_planet_exit(icao)
                    self.localAirspace.remove(icao)

    def handle_messages(self, messages):
        for msg, ts in messages:
            if len(msg) != 28:  # wrong data length
                continue

            df = pms.df(msg)

            if pms.crc(msg) !=0:  # CRC fail
                continue

            if df in (17,18):
                self.handle_adsb(msg)
            if df == 20:
                self.handle_modes(msg)
        self.handle_cleanup(datetime.now())

client = ADSBClient(host='10.208.42.113', port=30002, rawtype='raw')
client.run()

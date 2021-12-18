import time
import json
import math
import queue
import logging
import threading
import coloredlogs
import pyModeS as pms
import geopy.distance
import paho.mqtt.client as mqtt

from datetime import datetime, timedelta
from pyModeS.extra.tcpclient import TcpClient

mqtt_host = ("10.208.11.32",1883)
adsb_host = ("127.0.0.1", 30005, "beast")
mlat_host = ("127.0.0.1", 30105, "beast")
airspace_coords = (51.973357353305914, 5.669655084220917)

class ADSBClient(TcpClient):

    def __init__(self, host: str, port: int, rawtype: str, messageQueue: queue.Queue, handlerType: str):
        self.log = logging.getLogger(f"TCPClient-{handlerType}")
        self.log.info(f"Starting TCP Client ({host}:{port} @ {rawtype})")
        self.messageQueue = messageQueue
        self.handlerType = handlerType
        super(ADSBClient, self).__init__(host, port, rawtype)

    def handle_messages(self, messages):
        for msg, ts in messages:
            if len(msg) != 28 and len(msg) != 14:  # wrong data length
                print(len(msg))
                continue

            df = pms.df(msg)

            # Put the message, df and type of the listener in the queue
            self.messageQueue.put((msg, df, self.handlerType))

class mt_adsb():
    log = logging.getLogger("MAIN")

    PTDB = {}
    msgCache = {}
    running = False
    localAirspace = []
    mqtt = mqtt.Client()
    lastCleanup = time.time()
    messageQueue = queue.Queue()

    def start(self) -> None:
        self.running = True
        self.mqtt.connect(*mqtt_host)
        self.mqtt.loop_start()
        self.start_threads()
        self.process_messages()

    def stop(self) -> None:
        self.log.warning("Exiting...")
        self.mlatHandler.socket.disconnect("tcp://%s:%s" % (mlat_host[0], mlat_host[1]))
        self.adsbHandler.socket.disconnect("tcp://%s:%s" % (adsb_host[0], adsb_host[1]))
        self.mqtt.loop_stop()
        self.running = False
        self.log.info("Bye!")

    def calculate_initial_compass_bearing(self, pointA: tuple, pointB: tuple) -> int:
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

    def handle_plane_mlat_update(self, icao: str) -> None:
        self.mqtt.publish("space/planes/mlat", json.dumps(self.PTDB[icao]))

    def handle_plane_update(self, icao: str) -> None:
        self.mqtt.publish("space/planes/update", json.dumps(self.PTDB[icao]))

    def handle_plane_entry(self, icao: str) -> None:
        self.mqtt.publish("space/planes/geozone/enter", json.dumps(self.PTDB[icao]))
        self.log.info(f"{self.PTDB[icao]['callsign']} Entered our airspace ({json.dumps(self.PTDB[icao])}")

    def handle_plane_exit(self, icao: str) -> None:
        self.mqtt.publish("space/planes/geozone/exit", json.dumps(self.PTDB[icao]))
        self.log.info(f"{self.PTDB[icao]['callsign']} Exited our airspace")

    def handle_cleanup(self, ts: int) -> None:
        self.PTDB = {k:v for k,v in self.PTDB.items() if v['timestamp'] > (time.time() - 180)}
        self.msgCache = {k:v for k,v in self.msgCache.items() if v['timestamp'] > (ts - timedelta(minutes=3))}

    def handle_modes(self, msg):
        icao = pms.icao(msg)           # Infer the ICAO address from the message
        bds = pms.bds.infer(msg)
        if icao not in self.PTDB:
            self.PTDB[icao] = {
                    "icao": icao,
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
                    "source": []
                    }
        if bds == "BDS20":
            self.PTDB[icao]['callsign'] = pms.commb.cs20(msg).strip("_")
        if 'Mode-S' not in self.PTDB[icao]['source']:
            self.PTDB[icao]['source'].append('Mode-S')

    def handle_adsb(self, msg: str, handlerType: str) -> None:
        icao = pms.adsb.icao(msg)
        tc = pms.adsb.typecode(msg)

        if icao not in self.PTDB:
            self.PTDB[icao] = {
                    "icao": icao,
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
                    "mode": handlerType,
                    "source": []
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
            # Sometimes mlat can cause a bug in pms, so for now we just put this in a try-except
            #
            #   File "/usr/local/lib/python3.8/dist-packages/pyModeS/decoder/bds/bds09.py", line 69, in airborne_velocity
            # trk_or_hdg = round(trk, 2)
            # UnboundLocalError: local variable 'trk' referenced before assignment
            try:
                speed_heading = pms.adsb.speed_heading(msg)
                self.PTDB[icao]['speed'] = speed_heading[0]
                self.PTDB[icao]['heading'] = speed_heading[1]
            except:
                self.PTDB[icao]['speed'] = 0
                self.PTDB[icao]['heading'] = 0

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
                        self.msgCache[icao]['t_odd'], airspace_coords[0], airspace_coords[1])

                if not pos or not ((-90 < pos[0] < 90) and (-180 < pos[1] < 180)):
                    return

                self.PTDB[icao]['lat'] = pos[0]
                self.PTDB[icao]['lon'] = pos[1]
                self.PTDB[icao]['distance'] = geopy.distance.geodesic(airspace_coords, pos).km
                self.PTDB[icao]['bearing'] = self.calculate_initial_compass_bearing(tuple(airspace_coords), tuple(pos))

            #Send MLAT
            if (handlerType == "mlat"
                    and self.PTDB[icao]['lat'] != 0
                    and self.PTDB[icao]['lon'] != 0
                    ):
                self.handle_plane_mlat_update(icao)
                if self.PTDB[icao]['callsign'] == "":
                    self.PTDB[icao]['callsign'] = icao
                if 'MLAT' not in self.PTDB[icao]['source']:
                    self.PTDB[icao]['source'].append('MLAT')

            #Send update
            if (self.PTDB[icao]['callsign'] != ''
                    and self.PTDB[icao]['lat'] != 0
                    and self.PTDB[icao]['lon'] != 0
                    ):
                self.handle_plane_update(icao)

            #Update source if it was from ADS-B and not mlat
            if 'ADS-B' not in self.PTDB[icao]['source'] and handlerType != "mlat":
                self.PTDB[icao]['source'].append('ADS-B')

            #Local airspace stuff
            if (self.PTDB[icao]['distance'] < 5
                    and self.PTDB[icao]['alt'] < 15000
                    and self.PTDB[icao]['callsign'] != ""
                    and self.PTDB[icao]['lat'] != 0
                    and self.PTDB[icao]['lon'] != 0
                    ):
                if icao not in self.localAirspace:
                    self.PTDB[icao]['airspace'] = 1
                    self.PTDB[icao]['entered'] = time.time()
                    self.localAirspace.append(icao)
                    self.handle_plane_entry(icao)
            else:
                if icao in self.localAirspace:
                    self.PTDB[icao]['airspace'] = 0
                    self.PTDB[icao]['entered'] = 0
                    self.handle_plane_exit(icao)
                    self.localAirspace.remove(icao)


    def start_threads(self) -> None:
        self.mlatHandler = ADSBClient(*mlat_host, self.messageQueue, "mlat")
        self.adsbHandler = ADSBClient(*adsb_host, self.messageQueue, "adsb")

        self.mlatThread = threading.Thread(target=self.mlatHandler.run)
        self.adsbThread = threading.Thread(target=self.adsbHandler.run)

        self.mlatThread.daemon = True
        self.adsbThread.daemon = True

        self.mlatThread.start()
        self.adsbThread.start()

    def process_messages(self) -> None:
        self.log.info("Starting main processing queue")
        while self.running:
            # Constantly grab new data from the queue
            msg, df, handlerType = self.messageQueue.get()

            if df in (17,18) and pms.crc(msg) == 0:
                self.handle_adsb(msg, handlerType)
            elif df in (20,21):
                self.handle_modes(msg)
            elif df == 11: #Not implemented yet allcall
                continue
            elif df in (0,16): #No clue what this is
                continue
            else:
                self.log.info(f"DF: {df}")

            # Maybe we can just run this every minute or so instead?
            if self.lastCleanup - time.time() >= 60:
                self.lastCleanup = time.time()
                self.handle_cleanup(datetime.now())
                print(f"clean up ({self.lastCleanup - time.time()})")
            self.messageQueue.task_done()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    coloredlogs.install(level="INFO", fmt="%(asctime)s %(name)s %(levelname)s %(message)s")
    mtadsb = mt_adsb()
    try:
        mtadsb.start()
    except KeyboardInterrupt:
       mtadsb.stop()

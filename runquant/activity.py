from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, List
import os
import fitdecode
import json


@dataclass
class ActivityPoint:
    timestamp: datetime
    position_lat: int  # semicircles
    position_long: int  # semicircles
    distance: float  # meters
    heart_rate: int
    enhanced_speed: Optional[float] = None  # meters/second
    enhanced_altitude: Optional[float] = None  # meters
    cadence: Optional[int] = None
    unknown_135: Optional[int] = None
    unknown_143: Optional[int] = None
    power: Optional[int] = None
    unknown_140: Optional[int] = None

    def __post_init__(self):
        '''handle necessary conversions.

        timestamp are serialized as strings, so convert those back.
        fitfile native format for latlons is 'semicircles', so convert
        to degrees.
        '''
        if isinstance(self.timestamp, str):
            self.timestamp = convert_str_timestamp(self.timestamp)

        if not -90 <= self.position_lat <= 90:
            self.position_lat = self.position_lat * (
                180 / 2**31
            )  # convert to degrees
        if not -180 <= self.position_lat <= 180:
            self.position_long = self.position_long * (
                180 / 2**31
            )  # convert to degrees


def convert_str_timestamp(timestamp):
    if '+00:00' in timestamp:
        fmt = '%Y-%m-%d %H:%M:%S+00:00'
    else:
        fmt = '%Y-%m-%d %H:%M:%S'
    timestamp = datetime.strptime(timestamp, fmt)
    timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp


def load_fitfile(fn: str) -> List:
    with fitdecode.FitReader(fn) as fit:
        data = [frame for frame in fit if frame.frame_type == fitdecode.FIT_FRAME_DATA]

    return data


def ingest_activity_stream(fn: str):
    stream_keys = ['time', 'heartrate', 'latlng', 'altitude', 'watts']
    activity_key_lkup = {
        'time': 'timestamp',
        'heartrate': 'heart_rate',
        'altitude': 'enhanced_altitude',
        'watts': 'power',
    }
    with open(fn,'r') as f:
        stream = json.load(f)

    tstart = stream['timestamp']
    tstart = datetime.strptime(tstart, '%Y-%m-%dT%H:%M:%SZ')
    tstart = tstart.replace(tzinfo=timezone.utc).timestamp()

    ts = stream['time'][1]
    assert ts['type'] == 'time'
    activity = []
    for i in range(len(ts['data'])):
        pt = {}
        for sk in stream_keys:
            data = [s['data'] for s in stream[sk] if s['type'] == sk]
            assert len(data) == 1
            value = data[0][i]
            if sk == 'time':
                value = datetime.fromtimestamp(value + tstart)
                value.replace(tzinfo=timezone.utc)
            if sk == 'latlng':
                pt['position_lat'] = value[0]
                pt['position_long'] = value[1]
            else:
                pt[activity_key_lkup[sk]] = value
            if sk == 'time':
                data = [s['data'] for s in stream[sk] if s['type'] == 'distance']
                pt['distance'] = data[0][i]
        ap = ActivityPoint(**pt)
        activity.append(ap)
    act = Activity(activity)
    return act, stream['id']


def ingest_fitfile(fn: str):
    pass



class Activity:
    def __init__(self, points: List[ActivityPoint], user_id: int = 0):
        self.points = points
        self.user_id = user_id
        self._index = list(range(len(self.points)))
        self.fields = [k for k in self.points[0].__dict__.keys()]
        for k in self.fields:
            setattr(self, k, [getattr(pt, k) for pt in self.points])
        self.start_time = int(self.timestamp[0].timestamp())

    def save(self, save_dir: str):
        savename = os.path.join(
            save_dir, str(self.start_time) + '_' + str(self.user_id) + '.jsonl'
        )
        if not os.path.exists(savename):
            with open(savename, 'w') as f:
                for i in self._index:
                    tmp = {field: getattr(self, field)[i] for field in self.fields}
                    print(json.dumps(tmp, default=str), file=f)

    def calc_trimp(self):
        pass

from activity import load_fitfile, ActivityPoint, Activity
import argparse
import glob
import tqdm
import sqlite3
import os
import pandas as pd
from dataclasses import dataclass
from typing import Dict
import json
from utils import ActivityDB

''' data ingester from this.
assume a download archive directory, convert all of those fitfiles to jsons.

for example, 500 fitfiles take about 180s to read and parse.
same set of jsons take 5 seconds.
'''

def main(args):
    db = ActivityDB('./data/db')
    data = pd.read_csv(args.activities)
    fns = glob.glob(os.path.join(args.fitfiles, '*.fit'))
    data['filename'] = data['Filename'].apply(
        lambda x: x.split('/')[-1].replace('.gz', '') if x == x else None
    )
    for fn in tqdm.tqdm(fns):
        tmp = data.loc[data['filename'] == os.path.basename(fn)]

        stream = load_fitfile(fn)
        data_cols = list(ActivityPoint.__dataclass_fields__.keys())
        activity = []
        for row in stream:
            r = {
                d.name: row.get_value(d.def_num)
                for d in row.fields
                if d.name in data_cols
            }
            try:
                ap = ActivityPoint(**r)
                activity.append(ap)
            except:
                continue
        if len(activity) > 0:
            activity = Activity(activity)
            activity.save('./data/activities')

            meta = {
                'activity_id':None,
                'provider_id': int(tmp['Activity ID'].values[0]),
                'provider': 'strava',
                'athlete_id': activity.user_id,
                'start_time': activity.start_time,
            }
            db.insert_from_dict('activity_metadata',meta)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--fitfiles', '-f', type=str, help='directory where all fitfiles are located'
    )
    parser.add_argument(
        '--activities',
        '-a',
        type=str,
        help='path to the activity.csv file provided in strava data archive',
    )
    args = parser.parse_args()
    main(args)

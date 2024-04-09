import os
import requests
from flask import Flask, redirect, current_app, request
from datetime import datetime, timedelta
import json
from utils import ActivityDB
from activity import ingest_activity_stream
import shutil
import glob

app = Flask(__name__)

BASE_URL = 'https://www.strava.com/'

# TODO: app.config pattern
# TODO: sqlite for tokens and refresh token storage

app.config['PROVIDERS'] = {
    'strava': {
        'authorization_url': 'https://www.strava.com/oauth/authorize',
        'token_url': 'https://www.strava.com/oauth/token',
        'client_id': os.environ.get('STRAVA_CLIENT_ID'),
        'client_secret': os.environ.get('STRAVA_CLIENT_SECRET'),
    }
}
app.config['DB'] = {
    'path': '../runquant_data/data/db',
    'activities': 'activity_metadata',
}
app.config['STREAMS'] = ['time', 'heartrate', 'latlng', 'altitude', 'watts']


def get_activity_streams(token: str, activity_id: int):
    ret_streams = {}

    for s in current_app.config['STREAMS']:
        response = requests.get(
            f'https://www.strava.com/api/v3/activities/{activity_id}/streams?',
            params={'keys': s},
            headers={
                f'Authorization': 'Bearer ' + token,
                'Accept': 'application/json',
            },
        )
        if response.status_code == 200:
            ret_streams[s] = response.json()
        else:
            print(response.status_code, response.url)
            print('error on request.')
    return ret_streams


def process_activity_streams(
    db: ActivityDB, stream_dir: str = '../runquant_data/data/streams/*.json'
):
    stream_fns = glob.glob(stream_dir)
    print(len(stream_fns), 'streams to process')

    for fn in stream_fns:
        try:
            activity, prov_id = ingest_activity_stream(fn)
        except Exception as e:
            print(e)
            print(fn)

        print(prov_id)
        print(activity.start_time)
        activity.save('../runquant_data/data/activities')
        meta = {
            'activity_id': None,
            'provider_id': int(prov_id),
            'provider': 'strava',
            'athlete_id': activity.user_id,
            'timestamp': activity.start_time,
        }
        if not db.check_exists(meta['provider_id']):
            db.insert_from_dict('activity_metadata', meta)
        else:
            print('already in db.')
        shutil.move(
            fn,
            os.path.join('../runquant_data/archive/api_streams', os.path.basename(fn)),
        )


def get_recent_activities(token):
    after = (datetime.today() - timedelta(days=20)).timestamp()
    response = requests.get(
        f'https://www.strava.com/api/v3/athlete/activities?',
        params={'after': int(after)},
        headers={f'Authorization': 'Bearer ' + token, 'Accept': 'application/json'},
    )
    print(response.status_code)

    db = ActivityDB(current_app.config['DB']['path'])
    for activity in response.json():
        fn = f'../runquant_data/data/streams/{activity["id"]}.json'
        if not db.check_exists(activity['id']) and not os.path.exists(fn):
            print(f'downloading stream for new activity {activity["id"]}')
            streams = get_activity_streams(token, activity['id'])
            streams['id'] = activity['id']
            streams['timestamp'] = activity['start_date']
            with open(fn, 'w') as f:
                json.dump(streams, fp=f)
        else:
            print(f'already have that one.')

    process_activity_streams(db)
    return True


# def test_api(token):
#     after = datetime.strptime('2023-12-25', '%Y-%m-%d').timestamp()
#     test_id = '10440541659'
#     response = requests.get(
#         f'https://www.strava.com/api/v3/activities/{test_id}/streams?',
#         # params={'after':int(after)},
#         params={'keys':'heartrate'},
#         headers={f'Authorization': 'Bearer ' + token, 'Accept': 'application/json'},
#     )
#     print(response.status_code)
#     print(response.url)
#     with open('tmp2.json','w') as f:
#         json.dump(response.json(),fp=f)


@app.route('/oauth_callback')
def code_to_token_exchange():
    provider_data = current_app.config['PROVIDERS'].get('strava')
    # TODO check if code is there
    response = requests.post(
        provider_data['token_url'],
        data={
            'code': request.args['code'],
            'client_id': provider_data['client_id'],
            'client_secret': provider_data['client_secret'],
            'grant_type': 'authorization_code',
        },
    )
    token = response.json().get('access_token')
    refresh_token = response.json().get('refresh_token')
    print(token)
    print(refresh_token)
    if token is not None:
        _ = get_recent_activities(token)
        # TODO: redirect here to templated page for the dash.
        return 'Gathering data ...'
    else:
        return 'There was a problem...'


@app.route('/')
def get_access_code():
    provider_data = current_app.config['PROVIDERS'].get('strava')
    params = {
        'client_id': provider_data['client_id'],
        'response_type': 'code',
        'redirect_uri': 'http://127.0.0.1:5000/oauth_callback',
        'approval_prompt': 'auto',
        'scope': 'activity:read_all',
    }
    return redirect(build_api_url(provider_data['authorization_url'], params))


def build_api_url(endpoint, params=''):
    if params != '':
        params = '&'.join(['%s=%s' % (k, v) for k, v in params.items()])
    url = '%s?%s' % (endpoint, params)
    return url


if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1')

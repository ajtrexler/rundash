import numpy as np
from collections import Counter
from datetime import datetime, timedelta
from typing import Union

def align_hr_and_ts(activity):
    ''' impute hr values for every second of activity timeseries(ts)
    '''
    # convert datetimes to epoch timestamps and norm to t0
    ts = [int(t.timestamp()) for t in activity.timestamp]
    ts = np.array([t-ts[0] for t in ts])

    # get delta values between datapoints
    # and align heartrate stream to that
    delta_ts = (np.roll(ts,-1) - ts)[:-1]
    hr = activity.heart_rate[1:]
    assert len(hr) == len(delta_ts)

    # impute hr values at all seconds of the activity
    # this assumes seconds between datapoints t and t-1
    # have an hr value of t. 
    hr_ts = []
    for i,dts in enumerate(delta_ts):
        hr_ts.extend([hr[i]]*dts)
    return hr, ts, hr_ts

def calc_trimp_exp(activity, athlete):
    ''' calculate TRIMP-exp using heart rate reserve and exponential scaling.

    see: https://fellrnr.com/wiki/TRIMP
    '''
    hr, ts, hr_ts = align_hr_and_ts(activity)

    time_at_hrs = Counter(hr_ts)

    # calculate TRIMP using athlete hr info
    y_const = 1.92 if athlete.gender == 'm' else 1.67
    resting_hr = athlete.resting_hr
    max_hr = athlete.max_heart_rate(activity.timestamp[0])

    tmptrimps = []
    for hr,s in time_at_hrs.items():
        d = s / 60 # duration in minutes
        hrsubr = ((hr-resting_hr) / (max_hr-resting_hr)) # heart rate reserve
        scaling_factor = (0.64 * np.exp(hrsubr*y_const)) # exp scaling factor
        tmptrimps.append(d * hrsubr * scaling_factor)
    return sum(tmptrimps)

def calc_training_load(ti,ti_minus_one,decay_const):
    return (ti*(1-np.exp(-1/decay_const))) + (ti_minus_one*np.exp(-1/decay_const))

def calc_daily_trimp(activities, athlete):
    daily_trimp = {}
    for activity in activities:
        day = datetime.strftime(activity.timestamp[0].date(),'%Y-%m-%d')
        trimp = calc_trimp_exp(activity, athlete)
        if day not in daily_trimp:
            daily_trimp[day] = trimp
        else:
            daily_trimp[day] += trimp
    return daily_trimp

def model_tsb(daily_trimp, rel_date):

    # tsb running calc
    atl = [] # fatigue
    ctl = [] # fitness
    tsb = [] # performance
    atl_decay_const = 7
    ctl_decay_const = 42
    start = define_start_date(rel_date)
    for i in range(rel_date):
        day = datetime.strftime(start + timedelta(days=i),'%Y-%m-%d')
        if day not in daily_trimp:
            daily_value = 0
        else:
            daily_value = daily_trimp[day]
        atl.append(calc_training_load(daily_value,
                                    0 if i ==0 else atl[i-1],
                                    atl_decay_const))
        ctl.append(calc_training_load(daily_value,
                                    0 if i ==0 else ctl[i-1],
                                    ctl_decay_const))
        tsb.append(ctl[-1]-atl[-1])
    return ctl,atl,tsb

def define_start_date(rel_date: Union[int,datetime]) -> datetime:
    ''' convert a relative date to datetime
    '''
    if isinstance(rel_date,int):
        start = datetime.today() - timedelta(rel_date)
    else:
        start = rel_date
    return start

def model_bannister(daily_trimp, rel_date):
    performance = [0]
    fitness = [0]
    fatigue = [0]
    k1=1
    k2=1.9
    r1=49.5
    r2=11
    start = define_start_date(rel_date)
    for i in range(rel_date):
        day = datetime.strftime(start + timedelta(days=i),'%Y-%m-%d')
        if day not in daily_trimp:
            daily_value = 0
        else:
            daily_value = daily_trimp[day]
        fitness.append(fitness[-1]*np.exp(-1/r1) + daily_value)
        fatigue.append(fatigue[-1]*np.exp(-1/r2) + daily_value)
        performance.append(fitness[-1]*k1 - fatigue[-1]*k2)
    return fitness, fatigue, performance

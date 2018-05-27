#!/usr/bin/python3

import csv
import datetime
import getopt
import glob
import json
import os
import sys
import traceback
from decimal import *
from curwmysqladapter import MySQLAdapter, Data


def usage():
    usage_text = """
Usage: ./CSVTODAT.py [-d YYYY-MM-DD] [-t HH:MM:SS] [-h]

-h  --help          Show usage
-d  --date          Date in YYYY-MM-DD. Default is current date.
-t  --time          Time in HH:MM:SS. Default is current time.
    --start-date    Start date of timeseries which need to run the forecast in YYYY-MM-DD format. Default is same as -d(date).
    --start-time    Start time of timeseries which need to run the forecast in HH:MM:SS format. Default is same as -t(date).
-T  --tag           Tag to differential simultaneous Forecast Runs E.g. wrf1, wrf2 ...
    --wrf-rf        Path of WRF Rf(Rainfall) Directory. Otherwise using the `RF_DIR_PATH` from CONFIG.json
    --wrf-kub       Path of WRF kelani-upper-basin(KUB) Directory. Otherwise using the `KUB_DIR_PATH` from CONFIG.json
"""
    print(usage_text)


def get_timeseries(my_adapter, my_event_id, my_opts):
    existing_timeseries = my_adapter.retrieve_timeseries([my_event_id], my_opts)
    new_timeseries = []
    if len(existing_timeseries) > 0 and len(existing_timeseries[0]['timeseries']) > 0:
        existing_timeseries = existing_timeseries[0]['timeseries']
        prev_date_time = existing_timeseries[0][0]
        prev_sum = existing_timeseries[0][1]
        for tt in existing_timeseries:
            tt[0] = tt[0]
            if prev_date_time.replace(minute=0, second=0, microsecond=0) == tt[0].replace(minute=0, second=0,
                                                                                          microsecond=0):
                prev_sum += tt[1]  # TODO: If missing or minus -> ignore
                # TODO: Handle End of List
            else:
                new_timeseries.append([tt[0].replace(minute=0, second=0, microsecond=0), prev_sum])
                prev_date_time = tt[0]
                prev_sum = tt[1]
    return new_timeseries


def get_forecasted_timeseries(my_adapter, model_date_time, forecasted_id0, forecasted_id1, forecasted_id2): # eg: 2018-05-22 21:00:00
    forecast_d0_start = model_date_time - datetime.timedelta(hours=48)
    forecast_d0_end = model_date_time + datetime.timedelta(hours=0)
    forecast_d0_end = forecast_d0_end.strftime("%Y-%m-%d 23:00:00")
    forecast_d0_opts = {
        'from': forecast_d0_start.strftime("%Y-%m-%d %H:%M:%S"),
        'to': forecast_d0_end
    }
    forecast_d0_timeseries = get_timeseries(my_adapter, forecasted_id0, forecast_d0_opts)

    forecast_d1_start = datetime.datetime.strptime(forecast_d0_end, '%Y-%m-%d %H:%M:%S')
    forecast_d1_end = datetime.datetime.strptime(forecast_d0_end, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=24)
    forecast_d1_opts = {
        'from': forecast_d1_start.strftime("%Y-%m-%d %H:%M:%S"),
        'to': forecast_d1_end.strftime("%Y-%m-%d %H:%M:%S")
    }
    forecast_d1_timeseries = get_timeseries(my_adapter, forecasted_id1, forecast_d1_opts)
    forecast_d2_start = forecast_d1_end
    forecast_d2_end = forecast_d1_end + datetime.timedelta(hours=24)
    forecast_d2_opts = {
        'from': forecast_d2_start.strftime("%Y-%m-%d %H:%M:%S"),
        'to': forecast_d2_end.strftime("%Y-%m-%d %H:%M:%S")
    }
    forecast_d2_timeseries = get_timeseries(my_adapter, forecasted_id2, forecast_d2_opts)
    forecasted_timeseries = forecast_d0_timeseries + forecast_d1_timeseries + forecast_d2_timeseries

    forecast_end_time = model_date_time + datetime.timedelta(hours=72)
    print("forecast_end_time: ", forecast_end_time)
    last_avaialble_forecast_time = forecasted_timeseries[-1][0]

    print("last_avaialble_forecast_time : ", last_avaialble_forecast_time)
    print("last_avaialble_forecast_time type : ", type(last_avaialble_forecast_time))

    print("forecast_end_time : ", forecast_end_time)
    print("forecast_end_time type : ", type(forecast_end_time))

    while forecast_end_time > last_avaialble_forecast_time:
        next_forecast_time = last_avaialble_forecast_time + datetime.timedelta(hours=1)
        forecasted_timeseries.append([next_forecast_time, Decimal('0.0')])
        last_avaialble_forecast_time = next_forecast_time
    print("length forecasted_timeseries : ", len(forecasted_timeseries))
    print("forecasted_timeseries: ", forecasted_timeseries)

    return forecasted_timeseries


def get_observed_timeseries(my_adapter, model_date_time, observed_id):
    observed_start = model_date_time - datetime.timedelta(hours=48)
    observed_end = model_date_time
    observed_opts = {
        'from': observed_start.strftime("%Y-%m-%d %H:%M:%S"),
        'to': observed_end.strftime("%Y-%m-%d %H:%M:%S")
    }
    observed_timeseries = get_timeseries(my_adapter, observed_id, observed_opts)
    return observed_timeseries

def get_kub_mean_timeseries(my_adapter, model_date_time, observed_id, forecasted_id0, forecasted_id1, forecasted_id2):
    observed_timeseries = get_observed_timeseries(my_adapter, model_date_time, observed_id)
    forecasted_timeseries = get_forecasted_timeseries(my_adapter, model_date_time, forecasted_id0, forecasted_id1, forecasted_id2)
    timeseries_start = model_date_time - datetime.timedelta(hours=48)
    new_timeseries = []
    observe = 0
    for i in range(0, 119):
        if i == 0:
            timeseries_time = timeseries_start
        else:
            timeseries_time = timeseries_start + datetime.timedelta(hours=i+1)
        if timeseries_time == observed_timeseries[observe][0]:
            timeseries_element = observed_timeseries[observe]
            observe += 1
        else:
            timeseries_element = forecasted_timeseries[i]
        new_timeseries.append(timeseries_element)
    return new_timeseries

# Currently we don't have KLB obeserved values
def get_klb_mean_timeseries(my_adapter, model_date_time, forecasted_id0, forecasted_id1, forecasted_id2):
    forecasted_timeseries = get_forecasted_timeseries(my_adapter, model_date_time, forecasted_id0, forecasted_id1,
                                                      forecasted_id2)
    return forecasted_timeseries

try:
    CONFIG = json.loads(open('CONFIG.json').read())
    # print('Config :: ', CONFIG)
    RF_FORECASTED_DAYS = 0
    RAIN_CSV_FILE = 'DailyRain.csv'
    RF_DIR_PATH = './WRF/RF/'
    KUB_DIR_PATH = './WRF/kelani-upper-basin'
    OUTPUT_DIR = './OUTPUT'
    # Kelani Upper Basin
    kub_observed_id = 'b0e008522be904bcf71e290b3b0096b33c3e24d9b623dcbe7e58e7d1cc82d0db'
    kub_forecasted_id0 = 'fb575cb25f1e3d3a07c84513ea6a91c8f2fb98454df1a432518ab98ad7182861'  # wrf0, kub_mean, 0-d
    kub_forecasted_id1 = '9b18ffa16b251319ad1a931c4e1011b4ce42c874543def69b8a4af76d7b8f9fc'  # wrf0, kub_mean, 1-d
    kub_forecasted_id2 = 'e0e9cdc2aa4fef7178af08b987f4febc186d19397be744525fb6263815ca5fef'  # wrf0, kub_mean, 2-d
    # Kelani Lower Basin
    klb_observed_id = '69c464f749b36d9e55e461947238e7ed809c2033e75ae56234f466eec00aee35'  # wrf0, klb_mean, 0-d
    klb_forecasted_id0 = '69c464f749b36d9e55e461947238e7ed809c2033e75ae56234f466eec00aee35'  # wrf0, kub_mean, 0-d
    klb_forecasted_id1 = '35599583ae45d2c0ff93485b8a444da19fabdda8bf8fb539a6d77a0b0819da0a'  # wrf0, kub_mean, 1-d
    klb_forecasted_id2 = 'c48dbb9475ec31b3419bd3dd4206fdff3c53d4d156fa5681ccfa0768e4c39417'  # wrf0, kub_mean, 2-d

    MYSQL_HOST = "localhost"
    MYSQL_USER = "root"
    MYSQL_DB = "curw"
    MYSQL_PASSWORD = ""

    HEC_DATA_DIR = "/home/uwcc-admin/udp/hec_data/"

    if 'RF_FORECASTED_DAYS' in CONFIG:
        RF_FORECASTED_DAYS = CONFIG['RF_FORECASTED_DAYS']
    if 'RAIN_CSV_FILE' in CONFIG:
        RAIN_CSV_FILE = CONFIG['RAIN_CSV_FILE']
    if 'RF_DIR_PATH' in CONFIG:
        RF_DIR_PATH = CONFIG['RF_DIR_PATH']
    if 'KUB_DIR_PATH' in CONFIG:
        KUB_DIR_PATH = CONFIG['KUB_DIR_PATH']
    if 'OUTPUT_DIR' in CONFIG:
        OUTPUT_DIR = CONFIG['OUTPUT_DIR']
    if 'HEC_DATA_DIR' in CONFIG:
        HEC_DATA_DIR = CONFIG['HEC_DATA_DIR']

    if 'MYSQL_HOST' in CONFIG:
        MYSQL_HOST = CONFIG['MYSQL_HOST']
    if 'MYSQL_USER' in CONFIG:
        MYSQL_USER = CONFIG['MYSQL_USER']
    if 'MYSQL_DB' in CONFIG:
        MYSQL_DB = CONFIG['MYSQL_DB']
    if 'MYSQL_PASSWORD' in CONFIG:
        MYSQL_PASSWORD = CONFIG['MYSQL_PASSWORD']

    date = ''
    time = ''
    startDate = ''
    startTime = ''
    tag = ''
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hd:t:T:", [
            "help", "date=", "time=", "start-date=", "start-time=", "wrf-rf=", "wrf-kub=", "tag="
        ])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
            sys.exit()
        elif opt in ("-d", "--date"):  # model run date
            date = arg
        elif opt in ("-t", "--time"):  # model run time
            time = arg
        elif opt in ("--start-date"):
            startDate = arg
        elif opt in ("--start-time"):
            startTime = arg
        elif opt in ("--wrf-rf"):
            RF_DIR_PATH = arg
        elif opt in ("--wrf-kub"):
            KUB_DIR_PATH = arg
        elif opt in ("-T", "--tag"):
            tag = arg

    print('----------------------------------------------RFTOCSV run for : ', date, '@', time)
    model_date_time = datetime.datetime.strptime('%s %s' % (date, time), '%Y-%m-%d %H:%M:%S')

    # Get Observed Data
    adapter = MySQLAdapter(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DB)

    # KUB_Timeseries = get_observed_timeseries(adapter, KUB_OBS_ID, opts)
    KUB_Timeseries = get_kub_mean_timeseries(adapter, model_date_time, kub_observed_id, kub_forecasted_id0, kub_forecasted_id1, kub_forecasted_id2)
    if len(KUB_Timeseries) > 0:
        # print(KUB_Timeseries)
        print('KUB_Timeseries::', len(KUB_Timeseries), KUB_Timeseries[0], KUB_Timeseries[-1])
    else:
        print('No data found for KUB Obs timeseries: ', KUB_Timeseries)
    KLB_Timeseries = get_klb_mean_timeseries(adapter, model_date_time, klb_forecasted_id0, klb_forecasted_id1, klb_forecasted_id2)
    if len(KLB_Timeseries) > 0:
        # print(KLB_Timeseries)
        print('KLB_Timeseries::', len(KLB_Timeseries), KLB_Timeseries[0], KLB_Timeseries[-1])
    else:
        print('No data found for KLB Obs timeseries: ', KLB_Timeseries)

    print('Finished processing files. Start Writing Theissen polygon avg in to CSV')
    # print(UPPER_THEISSEN_VALUES)

    fileName = RAIN_CSV_FILE.rsplit('.', 1)
    # fileName = '{name}-{date}_{time}{tag}.{extention}'.format(name=fileName[0], date=date, time=time, tag='.' + tag if tag else '',
    #                                                    extention=fileName[1])
    fileName = '{name}-{date}{tag}.{extention}'.format(name=fileName[0], date=date,
                                                              tag='.' + tag if tag else '',
                                                              extention=fileName[1])
    RAIN_CSV_FILE_PATH = os.path.join(OUTPUT_DIR, fileName)
    # RAIN_CSV_FILE_PATH = os.path.join(HEC_DATA_DIR, fileName)
    csvWriter = csv.writer(open(RAIN_CSV_FILE_PATH, 'w'), delimiter=',', quotechar='|')
    # Write Metadata https://publicwiki.deltares.nl/display/FEWSDOC/CSV
    csvWriter.writerow(['Location Names', 'Awissawella', 'Colombo'])
    csvWriter.writerow(['Location Ids', 'Awissawella', 'Colombo'])
    csvWriter.writerow(['Time', 'Rainfall', 'Rainfall'])
    for i in range(0, 119):
        csvWriter.writerow([KUB_Timeseries[i][0].strftime('%Y-%m-%d %H:%M:%S'), "%.2f" % KUB_Timeseries[i][1], "%.2f" % KLB_Timeseries[i][1]])
        #print([KUB_Timeseries[i][0].strftime('%Y-%m-%d %H:%M:%S'), "%.2f" % KUB_Timeseries[i][1], "%.2f" % KLB_Timeseries[i][1]])
except ValueError:
    raise ValueError("Incorrect data format, should be YYYY-MM-DD")
except Exception as e:
    print(e)
    traceback.print_exc()
finally:
    print('Completed ', RF_DIR_PATH, ' to ', RAIN_CSV_FILE_PATH)

#!/usr/bin/python3

# Download Rain cell files, download Mean-ref files.
# Check Rain fall files are available in the bucket.

import fnmatch
import sys
import os
import json
import getopt
import datetime
from google.cloud import storage
from curw.rainfall.wrf.extraction import observation_utils as utils
from curwmysqladapter import MySQLAdapter, Data


def usage():
    usage_text = """
Usage: ./CSVTODAT.py [-d YYYY-MM-DD] [-t HH:MM:SS] [-h]

-h  --help          Show usage
-d  --date          Date in YYYY-MM-DD. 
-t  --time          Time in HH:00:00.
-f  --forward       Future day count
-b  --backward      Past day count
-T  --tag           Tag to differential simultaneous Forecast Runs E.g. wrf1, wrf2 ...
    --wrf-rf        Path of WRF Rf(Rainfall) Directory. Otherwise using the `RF_DIR_PATH` from CONFIG.json
    --wrf-kub       Path of WRF kelani-upper-basin(KUB) Directory. Otherwise using the `KUB_DIR_PATH` from CONFIG.json
"""
    print(usage_text)

def download_wrf_files():
    try:
        client = storage.Client.from_service_account_json(key_file)
        bucket = client.get_bucket(bucket_name)
        prefix = initial_path_prefix + '_'
        blobs = bucket.list_blobs(prefix=prefix)
        print("prefix : ", prefix)
        print("net_cdf_file_name : ", net_cdf_file_name)
        for blob in blobs:
            if fnmatch.fnmatch(blob.name, "*" + net_cdf_file_name):
                print(blob.name)
                directory = download_location + "/" + net_cdf_file_name
                if not os.path.exists(download_location):
                    os.makedirs(download_location)
                blob.download_to_filename(directory)
                download_raincell_file(directory)
                return True
        return False
    except:
        print('Rain cell/Mean-Ref/Rain fall file download failed')
        return False


def download_raincell_file(net_cdf_file_name):
    adapter = MySQLAdapter(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DB)
    #start_ts_lk = '2018-05-24_08:00'
    obs_stations = {'Kottawa North Dharmapala School': [79.95818, 6.865576, 'A&T Labs', 'wrf_79.957123_6.859688'],
                    'IBATTARA2': [79.919, 6.908, 'CUrW IoT', 'wrf_79.902664_6.913757'],
                    'Malabe': [79.95738, 6.90396, 'A&T Labs', 'wrf_79.957123_6.913757'],
                    'Mutwal': [79.8609, 6.95871, 'A&T Labs', 'wrf_79.875435_6.967812'],
                    'Mulleriyawa': [79.941176, 6.923571, 'A&T Labs', 'wrf_79.929893_6.913757'],
                    'Orugodawatta': [79.87887, 6.943741, 'CUrW IoT', 'wrf_79.875435_6.940788']}
    kelani_lower_basin_points = None
    utils.extract_kelani_basin_rainfall_flo2d_with_obs(net_cdf_file_name, adapter, obs_stations,
                                                       download_location, start_ts_lk,
                                                 kelani_lower_basin_points=kelani_lower_basin_points,
                                                 duration_days=duration_days)


try:
    run_date = datetime.datetime.now().strftime("%Y-%m-%d")
    run_time = datetime.datetime.now().strftime("%H:00:00")
    tag = ''
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hd:t:T:f:b:", [
            "help", "date=", "time=", "forward=", "backward=", "wrf-rf=", "wrf-kub=", "tag="
        ])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
            sys.exit()
        elif opt in ("-d", "--date"):
            run_date = arg # 2018-05-24
        elif opt in ("-t", "--time"):
            run_time = arg # 16:00:00
        elif opt in ("-f","--forward"):
            forward = arg
        elif opt in ("-b","--backward"):
            backward = arg
        elif opt in ("--wrf-rf"):
            RF_DIR_PATH = arg
        elif opt in ("--wrf-kub"):
            KUB_DIR_PATH = arg
        elif opt in ("-T", "--tag"):
            tag = arg
    print("WrfTrigger run_date : ", run_date)
    print("WrfTrigger run_time : ", run_time)
    start_ts_lk = datetime.datetime.strptime('%s %s' % (run_date, run_time), '%Y-%m-%d %H:%M:%S')
    start_ts_lk = start_ts_lk.strftime('%Y-%m-%d_%H:00')  # '2018-05-24_08:00'
    print("WrfTrigger start_ts_lk : ", start_ts_lk)
    duration_days = (int(backward), int(forward))
    print("WrfTrigger duration_days : ", duration_days)
    with open('CONFIG.json') as json_file:
        config_data = json.load(json_file)
        key_file = config_data["KEY_FILE_PATH"]
        bucket_name = config_data["BUCKET_NAME"]
        initial_path_prefix = config_data["INITIAL_PATH_PREFIX"]
        net_cdf_file_format = config_data["NET_CDF_FILE"]
        wrf_data_dir = config_data["WRF_DATA_DIR"]
        net_cdf_date = datetime.datetime.strptime(run_date, '%Y-%m-%d') - datetime.timedelta(hours=24)
        net_cdf_date = net_cdf_date.strftime("%Y-%m-%d")
        download_location = wrf_data_dir + run_date + '_' + run_time
        print("download_location : ", download_location)
        print("net_cdf_date : ", net_cdf_date)

        MYSQL_HOST = config_data['MYSQL_HOST']
        MYSQL_USER = config_data['MYSQL_USER']
        MYSQL_DB = config_data['MYSQL_DB']
        MYSQL_PASSWORD = config_data['MYSQL_PASSWORD']

        name_list = net_cdf_file_format.split("-")
        net_cdf_file_name = name_list[0] + "_" + net_cdf_date + "_" + name_list[1]
        try:
            if True == download_wrf_files():
                print("proceed")
            else:
                print("try again later.")
        except Exception as ex:
            print("Download required files|Exception: ", ex)
except Exception as e:
    print("Exception occurred: ", e)

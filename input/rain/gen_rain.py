#!"D:\curw_flo2d_data_manager\venv\Scripts\python.exe"
import pymysql
import getopt
from datetime import datetime, timedelta
import traceback
import os
import sys, re
import json
import csv
import pandas as pd

from db_adapter.constants import set_db_config_file_path
from db_adapter.constants import connection as con_params
from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.curw_sim.timeseries import Timeseries


DATE_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

# from db_adapter.constants import CURW_SIM_DATABASE, CURW_SIM_PASSWORD, CURW_SIM_USERNAME, CURW_SIM_PORT, CURW_SIM_HOST
ROOT_DIRECTORY = 'D:\curw_flo2d_data_manager'


def write_to_file(file_name, data):
    with open(file_name, 'w+') as f:
        f.write('\n'.join(data))


def list_of_lists_to_df_first_row_as_columns(data):
    return pd.DataFrame.from_records(data[1:], columns=data[0])


def get_SL_time_now():
    return  (datetime.utcnow() + timedelta(hours=5, minutes=30))


def save_metadata_to_file(input_filepath, metadata):
    metadata_filepath = os.path.join(os.path.dirname(input_filepath), "run_meta.json")

    updated_metadata = {}
    try:
        existing_metadata = json.loads(open(metadata_filepath).read())
        updated_metadata = existing_metadata
    except FileNotFoundError as eFNFE:
        pass

    for key in metadata.keys():
        updated_metadata[key] = metadata[key]

    with open(metadata_filepath, 'w') as outfile:
        json.dump(updated_metadata, outfile)


def append_to_file(file_name, data):
    with open(file_name, 'a+') as f:
        f.write('\n'.join(data))


def makedir_if_not_exist_given_filepath(filename):
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc:  # Guard against race condition
            pass


def read_attribute_from_config_file(attribute, config, compulsory=False):
    """
    :param attribute: key name of the config json file
    :param config: loaded json file
    :param compulsory: Boolean value: whether the attribute is must present or not in the config file
    :return:

    """
    if attribute in config and (config[attribute]!=""):
        return config[attribute]
    elif compulsory:
        print("{} not specified in config file.".format(attribute))
        exit(1)
    else:
        print("{} not specified in config file.".format(attribute))
        return None

def check_time_format(time, model):
    try:
        pattern_10m = re.compile('flo2d_10_+')

        time = datetime.strptime(time, DATE_TIME_FORMAT)

        if time.strftime('%S') != '00':
            print("Seconds should be always 00")
            exit(1)
        if model == "flo2d_250" and time.strftime('%M') not in (
        '05', '10', '15', '20', '25', '30', '35', '40', '45', '50', '55', '00'):
            print("Minutes should be multiple of 5 fro flo2d_250")
            exit(1)
        if pattern_10m.match(model) and time.strftime('%M') not in (
        '05', '10', '15', '20', '25', '30', '35', '40', '45', '50', '55', '00'):
            print("Minutes should be multiple of 5 fro flo2d_10m models")
            exit(1)
        if model in ("flo2d_150", "flo2d_150_v2") and time.strftime('%M') not in ('15', '30', '45', '00'):
            print("Minutes should be multiple of 15 for flo2d_150")
            exit(1)

        return True
    except Exception:
        traceback.print_exc()
        print("Time {} is not in proper format".format(time))
        exit(1)


def replace_negative_numbers_with_nan(df):
    num = df._get_numeric_data()
    num[num < 0] = np.nan
    return df


def find_hash_id_of_nearest_rainfall_station(curw_obs_pool, curw_sim_pool, lat, lon):

    obs_connection = curw_obs_pool.connection()
    Sim_Ts = Timeseries(pool=curw_sim_pool)

    try:
        with obs_connection.cursor() as cursor0:
            cursor0.callproc('getNearestWeatherStation', (lat, lon))
            obs_station = cursor0.fetchone()
            obs_id = obs_station['id']
            obs_station_name = obs_station['name']
            grid_id = 'rainfall_{}_{}_MDPA'.format(obs_id, obs_station_name) # rainfall_100057_Naula_MDPA

        return Sim_Ts.get_timeseries_id(grid_id=grid_id, method='MME')

    except Exception as e:
        traceback.print_exc()


def prepare_rain(curw_sim_pool, rain_file_path, curw_sim_hash_id, start_time, end_time, target_model):

    # retrieve observed timeseries
    df = pd.DataFrame()
    df['time'] = pd.date_range(start=start_time, end=end_time, freq='5min')

    TS = Timeseries(curw_sim_pool)
    ts = TS.get_timeseries(id_=curw_sim_hash_id, start_date=start_time, end_date=end_time)
    ts.insert(0, ['time', 'value'])

    ts_df = list_of_lists_to_df_first_row_as_columns(ts)
    ts_df['value'] = ts_df['value'].astype('float64')

    df = pd.merge(df, ts_df, how="left", on='time')
    df.set_index('time', inplace=True)
    df = df.dropna()

    if target_model == "flo2d_250":
        timestep = 5
    elif target_model in ("flo2d_150", "flo2d_150_v2"):
        timestep = 15
    else:
        timestep = 5

    if timestep == 15:
        df = df.resample('15min', label='right', closed='right').sum()

    df = replace_negative_numbers_with_nan(df)

    timeseries = df['value'].reset_index().values.tolist()

    start_time = datetime.strptime(start_time, DATE_TIME_FORMAT)

    rain_dat = []

    total_rain = 0

    cumulative_timeseries = []

    for i in range(len(timeseries)):
        total_rain += float(timeseries[i][1])
        cumulative_timeseries.append(total_rain)

    for i in range(len(timeseries)):
        time_col = '%.3f' % (((timeseries[i][0] - start_time).total_seconds()) / 3600)
        if total_rain > 0:
            rain_col = '%.3f' % (cumulative_timeseries[i] / total_rain)
        else:
            rain_col = '%.3f' % (0)

        rain_dat.append("R              " + time_col.ljust(14) + rain_col + " ")

    rain_dat.insert(0, " {}         5             0             0 ".format('%.3f' % total_rain))
    rain_dat.insert(0, " 0             0 ")

    write_to_file(rain_file_path, rain_dat)


def create_dir_if_not_exists(path):
    """
    create directory(if needed recursively) or paths
    :param path: string : directory path
    :return: string
    """
    if not os.path.exists(path):
        os.makedirs(path)

    return path


def usage():
    usageText = """
    -----------------------------------------------------
    Prepare rain for Flo2D 250, 150, 150_v2 & 10m models
    -----------------------------------------------------

    Usage: .\input\\rain\\gen_rain.py [-m flo2d_XXX][-s "YYYY-MM-DD HH:MM:SS"] [-e "YYYY-MM-DD HH:MM:SS"] [-d "directory_path"] 
    [-h XXXXXXXXXX] [-E]

    -h  --help          Show usage
    -m  --model         FLO2D model (e.g. flo2d_250, flo2d_150). Default is flo2d_250.
    -s  --start_time    Rain start time (e.g: "2019-06-05 00:00:00"). Default is 23:30:00, 3 days before today.
    -e  --end_time      Rain end time (e.g: "2019-06-05 23:30:00"). Default is 23:30:00, tomorrow.
    -d  --dir           Rain file generation location (e.g: "C:\\udp_150\\2019-09-23")
    -h  --hash_id       Curw sim hash id of the desired timeseries
    -E  --event_sim     Weather the rain is prepared for event simulation or not (e.g. -E, --event_sim)
    """
    print(usageText)


if __name__ == "__main__":

    set_db_config_file_path(os.path.join(ROOT_DIRECTORY, 'db_adapter_config.json'))

    try:
        pattern_10m = re.compile('flo2d_10_+')
        start_time = None
        end_time = None
        flo2d_model = None
        curw_sim_hash_id = None
        output_dir = None
        file_name = 'RAIN.DAT'
        event_sim = False

        try:
            opts, args = getopt.getopt(sys.argv[1:], "h:m:s:e:d:h:E",
                                       ["help", "flo2d_model=", "start_time=", "end_time=", "dir=", "hash_id=",
                                        "event_sim"])
        except getopt.GetoptError:
            usage()
            sys.exit(2)
        for opt, arg in opts:
            if opt in ("-h", "--help"):
                usage()
                sys.exit()
            elif opt in ("-m", "--flo2d_model"):
                flo2d_model = arg.strip()
            elif opt in ("-s", "--start_time"):
                start_time = arg.strip()
            elif opt in ("-e", "--end_time"):
                end_time = arg.strip()
            elif opt in ("-d", "--dir"):
                output_dir = arg.strip()
            elif opt in ("-h", "--hash_id"):
                curw_sim_hash_id = arg.strip()
            elif opt in ("-E", "--event_sim"):
                event_sim = True

        if event_sim:
            set_db_config_file_path(os.path.join(ROOT_DIRECTORY, 'db_adapter_config_event_sim.json'))

        if flo2d_model is None:
            flo2d_model = "flo2d_250"
        elif not (flo2d_model in ("flo2d_250", "flo2d_150", "flo2d_150_v2") or pattern_10m.match(flo2d_model)):
            print("Flo2d model should be either \"flo2d_250\" or \"flo2d_150\" or \"flo2d_150_v2\" or \"flo2d_10_*\"")
            exit(1)

        if start_time is None:
            start_time = (get_SL_time_now() - timedelta(hours=3)).strftime('%Y-%m-%d %H:00:00')
        else:
            check_time_format(time=start_time, model=flo2d_model)

        if end_time is None:
            end_time = (datetime.now() + timedelta(days=2)).strftime('%Y-%m-%d %H:00:00')
        else:
            check_time_format(time=end_time, model=flo2d_model)

        if end_time < start_time:
            print("start_time should be less than end_time")
            exit(1)

        if output_dir is not None:
            rain_file_path = os.path.join(output_dir, file_name)
        else:
            rain_file_path = os.path.join(r"D:\rain",
                                              'RAIN_{}_{}_{}.DAT'.format(flo2d_model, start_time, end_time).replace(
                                                  ' ', '_').replace(':', '-'))

        curw_sim_pool = get_Pool(host=con_params.CURW_SIM_HOST, user=con_params.CURW_SIM_USERNAME,
                                 password=con_params.CURW_SIM_PASSWORD,
                                 port=con_params.CURW_SIM_PORT, db=con_params.CURW_SIM_DATABASE)

        curw_obs_pool = get_Pool(host=con_params.CURW_OBS_HOST, user=con_params.CURW_OBS_USERNAME,
                                 password=con_params.CURW_OBS_PASSWORD,
                                 port=con_params.CURW_OBS_PORT, db=con_params.CURW_OBS_DATABASE)

        makedir_if_not_exist_given_filepath(rain_file_path)

        if not os.path.isfile(rain_file_path):
            if pattern_10m.match(flo2d_model):
                config = json.loads(open(os.path.join(ROOT_DIRECTORY, "input", "rain", "config_flo2d_10.json")).read())
                model_10m = read_attribute_from_config_file(flo2d_model, config, True)
                lat = model_10m.get('lat')
                lon = model_10m.get('lon')
                curw_sim_hash_id = find_hash_id_of_nearest_rainfall_station(curw_obs_pool=curw_obs_pool,
                                                                            curw_sim_pool=curw_sim_pool,
                                                                            lat=lat, lon=lon)
            else:
                if curw_sim_hash_id is None:
                    print("Curw sim hash id of the desired timeseries is not specified")
                    exit(1)

            print("{} start preparing rain".format(datetime.now()))
            prepare_rain(curw_sim_pool=curw_sim_pool, rain_file_path=rain_file_path, curw_sim_hash_id=curw_sim_hash_id,
                         start_time=start_time, end_time=end_time, target_model=flo2d_model)
            metadata = {
                "rain": {
                    "hash_id": curw_sim_hash_id,
                    "model": flo2d_model
                }
            }
            save_metadata_to_file(input_filepath=rain_file_path, metadata=metadata)
            print("{} completed preparing rain".format(datetime.now()))
        else:
            print('Raincell file already in path : ', rain_file_path)

    except Exception:
        traceback.print_exc()
    finally:
        destroy_Pool(pool=curw_sim_pool)
        destroy_Pool(pool=curw_obs_pool)

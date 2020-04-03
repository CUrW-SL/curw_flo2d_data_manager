#!"D:\curw_flo2d_data_manager\venv\Scripts\python.exe"
import pymysql
from datetime import datetime, timedelta
import traceback
import json
import os
import sys
import getopt

DATE_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
ROOT_DIRECTORY = 'D:\curw_flo2d_data_manager'

from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.constants import set_db_config_file_path
from db_adapter.constants import connection as con_params
# from db_adapter.constants import CURW_SIM_DATABASE, CURW_SIM_HOST, CURW_SIM_PASSWORD, CURW_SIM_PORT, CURW_SIM_USERNAME
# from db_adapter.constants import CURW_OBS_DATABASE, CURW_OBS_PORT, CURW_OBS_PASSWORD, CURW_OBS_USERNAME, CURW_OBS_HOST
from db_adapter.curw_sim.timeseries import get_curw_sim_discharge_id
from db_adapter.curw_sim.timeseries.discharge import Timeseries as DisTS
from db_adapter.constants import COMMON_DATE_TIME_FORMAT


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


def write_to_file(file_name, data):
    with open(file_name, 'w+') as f:
        f.write('\n'.join(data))


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


def check_time_format(time):
    try:
        time = datetime.strptime(time, DATE_TIME_FORMAT)

        if time.strftime('%S') != '00':
            print("Seconds should be always 00")
            exit(1)
        if time.strftime('%M') != '00':
            print("Minutes should be always 00")
            exit(1)

        return True
    except Exception:
        print("Time {} is not in proper format".format(time))
        exit(1)


def prepare_inflow(inflow_file_path, start, end, discharge_id, wl_id, curw_sim_pool):

    obs_wl = None

    try:

        curw_obs_pool = get_Pool(host=con_params.CURW_OBS_HOST, user=con_params.CURW_OBS_USERNAME,
                                 password=con_params.CURW_OBS_PASSWORD, port=con_params.CURW_OBS_PORT,
                                 db=con_params.CURW_OBS_DATABASE)

        connection = curw_obs_pool.connection()

        # Extract waterlevel
        with connection.cursor() as cursor1:
            obs_end = datetime.strptime(start, COMMON_DATE_TIME_FORMAT) + timedelta(hours=10)
            cursor1.callproc('getWL', (wl_id, start, obs_end))
            result = cursor1.fetchone()
            obs_wl = result.get('value')

        if obs_wl is None:
            obs_wl = 0.5

        # Extract discharge series
        TS = DisTS(pool=curw_sim_pool)
        discharge_ts = TS.get_timeseries(id_=discharge_id, start_date=start, end_date=end)

        inflow = []

        inflow.append('0               0')
        inflow.append('C               0            8655')
        inflow.append('H               0               0')

        timeseries = discharge_ts
        for i in range(1, len(timeseries)):
            time_col = (str('%.1f' % (((timeseries[i][0] - timeseries[0][0]).total_seconds())/3600))).rjust(16)
            value_col = (str('%.1f' % (timeseries[i][1]))).rjust(16)
            inflow.append('H' + time_col + value_col)

        inflow.append('R            2265{}'.format((str(obs_wl)).rjust(16)))
        inflow.append('R            3559             6.6')

        write_to_file(inflow_file_path, data=inflow)

    except Exception as e:
        print(traceback.print_exc())
    finally:
        connection.close()
        destroy_Pool(curw_obs_pool)
        destroy_Pool(curw_sim_pool)
        print("Inflow generated")


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
    -----------------------------
    Prepare inflow for Flo2D 250
    -----------------------------
    
    Usage: .\input\inflow\gen_250_inflow.py [-s "YYYY-MM-DD HH:MM:SS"] [-e "YYYY-MM-DD HH:MM:SS"] [-d "directory_path"] 
    [-M XXX] [-E]

    -h  --help          Show usage
    -s  --start_time    Inflow start time (e.g: "2019-06-05 00:00:00"). Default is 00:00:00, 2 days before today.
    -e  --end_time      Inflow end time (e.g: "2019-06-05 23:00:00"). Default is 00:00:00, tomorrow.
    -d  --dir           Inflow file generation location (e.g: "C:\\udp_150\\2019-09-23")
    -M  --method        Inflow calculation method (e.g: "MME", "SF", "OBS")
    -E  --event_sim     Weather the inflow is prepared for event simulation or not (e.g. -E, --event_sim)
    """
    print(usageText)


if __name__ == "__main__":

    set_db_config_file_path(os.path.join(ROOT_DIRECTORY, 'db_adapter_config.json'))

    try:

        GRID_ID = "discharge_hanwella"

        start_time = None
        end_time = None
        method = None
        output_dir = None
        file_name = 'INFLOW.DAT'
        flo2d_model = 'flo2d_250'
        event_sim = False

        try:
            opts, args = getopt.getopt(sys.argv[1:], "h:s:e:d:M:E",
                                       ["help", "start_time=", "end_time=", "dir=", "method=", "event_sim"])
        except getopt.GetoptError:
            usage()
            sys.exit(2)
        for opt, arg in opts:
            if opt in ("-h", "--help"):
                usage()
                sys.exit()
            elif opt in ("-s", "--start_time"):
                start_time = arg.strip()
            elif opt in ("-e", "--end_time"):
                end_time = arg.strip()
            elif opt in ("-d", "--dir"):
                output_dir = arg.strip()
            elif opt in ("-M", "--method"):
                method = arg.strip()
            elif opt in ("-E", "--event_sim"):
                event_sim = True

        if event_sim:
            set_db_config_file_path(os.path.join(ROOT_DIRECTORY, 'db_adapter_config_event_sim.json'))

        # Load config details and db connection params
        config = json.loads(open(os.path.join(ROOT_DIRECTORY, "input", "inflow", "config_250.json")).read())

        curw_sim_pool = get_Pool(host=con_params.CURW_SIM_HOST, user=con_params.CURW_SIM_USERNAME,
                                 password=con_params.CURW_SIM_PASSWORD, port=con_params.CURW_SIM_PORT,
                                 db=con_params.CURW_SIM_DATABASE)

        if method is None:
            discharge_id = read_attribute_from_config_file('discharge_id', config, True)
        else:
            discharge_id = get_curw_sim_discharge_id(pool=curw_sim_pool, method=method, model=flo2d_model,
                                                     grid_id=GRID_ID)
        print(discharge_id)
        wl_id = read_attribute_from_config_file('wl_id', config, True)

        if start_time is None:
            start_time = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d 00:00:00')
        else:
            check_time_format(time=start_time)

        if end_time is None:
            end_time = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
        else:
            check_time_format(time=end_time)

        if output_dir is not None:
            inflow_file_path = os.path.join(output_dir, file_name)
        else:
            inflow_file_path = os.path.join(r"D:\inflow",
                                          'INFLOW_flo2d_250_{}_{}.DAT'.format(start_time, end_time).replace(' ', '_').replace(':', '-'))

        makedir_if_not_exist_given_filepath(inflow_file_path)

        if not os.path.isfile(inflow_file_path):
            print("{} start preparing inflow".format(datetime.now()))
            prepare_inflow(inflow_file_path, start=start_time, end=end_time, discharge_id=discharge_id, wl_id=wl_id,
                           curw_sim_pool=curw_sim_pool)
            metadata = {
                "inflow": {
                    "tag": method,
                    "model": flo2d_model,
                    "discharge_id": discharge_id
                }
            }
            save_metadata_to_file(input_filepath=inflow_file_path, metadata=metadata)
            print("{} completed preparing inflow".format(datetime.now()))
        else:
            print('Inflow file already in path : ', inflow_file_path)

        # os.system(r"deactivate")

    except Exception:
        traceback.print_exc()


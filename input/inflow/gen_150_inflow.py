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
from db_adapter.curw_sim.timeseries import get_curw_sim_discharge_id
from db_adapter.curw_sim.timeseries.discharge import Timeseries as DisTS


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


def prepare_inflow_150(inflow_file_path, start, end, discharge_id, curw_sim_pool):

    try:

        # Extract discharge series
        TS = DisTS(pool=curw_sim_pool)
        discharge_ts = TS.get_timeseries(id_=discharge_id, start_date=start, end_date=end)

        inflow = []

        inflow.append('0           41550')
        inflow.append('C               0           41550')
        inflow.append('H               0               0')

        timeseries = discharge_ts
        for i in range(1, len(timeseries)):
            time_col = (str('%.1f' % (((timeseries[i][0] - timeseries[0][0]).total_seconds())/3600))).rjust(16)
            value_col = (str('%.1f' % (timeseries[i][1]))).rjust(16)
            inflow.append('H' + time_col + value_col)

        write_to_file(inflow_file_path, data=inflow)

    except Exception as e:
        print(traceback.print_exc())
    finally:
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
    ----------------------------
    Prepare inflow for Flo2D 150
    ----------------------------
    
    Usage: .\input\inflow\gen_150_inflow.py [-s "YYYY-MM-DD HH:MM:SS"] [-e "YYYY-MM-DD HH:MM:SS"] [-d "directory_path"] [-M XXX]

    -h  --help          Show usage
    -s  --start_time    Inflow start time (e.g: "2019-06-05 00:00:00"). Default is 00:00:00, 2 days before today.
    -e  --end_time      Inflow end time (e.g: "2019-06-05 23:00:00"). Default is 00:00:00, tomorrow.
    -d  --dir           Inflow file generation location (e.g: "C:\\udp_150\\2019-09-23")
    -M  --method        Inflow calculation method (e.g: "MME", "SF")
    """
    print(usageText)


if __name__ == "__main__":

    set_db_config_file_path(os.path.join(ROOT_DIRECTORY, 'db_adapter_config.json'))

    try:

        GRID_ID = "discharge_glencourse"

        start_time = None
        end_time = None
        method = None
        output_dir = None
        file_name = 'INFLOW.DAT'
        flo2d_model = 'flo2d_150'

        try:
            opts, args = getopt.getopt(sys.argv[1:], "h:s:e:d:M:",
                                       ["help", "start_time=", "end_time=", "dir=", "method="])
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

        # Load config details and db connection params
        config = json.loads(open(os.path.join(ROOT_DIRECTORY, "input", "inflow", "config_150.json")).read())

        curw_sim_pool = get_Pool(host=con_params.CURW_SIM_HOST, user=con_params.CURW_SIM_USERNAME,
                                 password=con_params.CURW_SIM_PASSWORD, port=con_params.CURW_SIM_PORT,
                                 db=con_params.CURW_SIM_DATABASE)

        if method is None:
            discharge_id = read_attribute_from_config_file('discharge_id', config, True)
        else:
            discharge_id = get_curw_sim_discharge_id(pool=curw_sim_pool, method=method, model=flo2d_model,
                                                     grid_id=GRID_ID)

        print(discharge_id)

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
                                          'INFLOW_flo2d_150_{}_{}.DAT'.format(start_time, end_time).replace(' ', '_').replace(':', '-'))

        makedir_if_not_exist_given_filepath(inflow_file_path)

        if not os.path.isfile(inflow_file_path):
            print("{} start preparing inflow".format(datetime.now()))
            prepare_inflow_150(inflow_file_path, start=start_time, end=end_time, discharge_id=discharge_id,
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


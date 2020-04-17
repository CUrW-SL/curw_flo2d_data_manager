#!"D:\curw_flo2d_data_manager\venv\Scripts\python.exe"
import traceback, os, getopt, sys
import json

from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.constants import set_db_config_file_path
from db_adapter.constants import connection as con_params
from db_adapter.curw_fcst.source import get_source_id, add_source
from db_adapter.curw_fcst.variable import get_variable_id, add_variable
from db_adapter.curw_fcst.unit import get_unit_id, add_unit, UnitType
from db_adapter.curw_fcst.station import add_station, StationEnum
# from db_adapter.constants import CURW_FCST_HOST, CURW_FCST_USERNAME, CURW_FCST_PASSWORD, CURW_FCST_PORT, CURW_FCST_DATABASE
from db_adapter.curw_sim.constants import FLO2D_150_V2

from db_adapter.csv_utils import read_csv

ROOT_DIRECTORY = 'D:\curw_flo2d_data_manager'


def usage():
    usageText = """
    --------------------------------------------------------------------
    Extract Flo2D 250 & 150 output discharge to the curw_fcst database.
    --------------------------------------------------------------------

    Usage: .\init\\init.py [-E]

    -h  --help          Show usage
    -E  --event_sim     Weather the initialization is for event simulation or not (e.g. -E, --event_sim)
    """
    print(usageText)


if __name__=="__main__":

    set_db_config_file_path(os.path.join(ROOT_DIRECTORY, 'db_adapter_config.json'))

    try:

        event_sim = False

        try:
            opts, args = getopt.getopt(sys.argv[1:], "h:E",
                                       ["help", "event_sim"])
        except getopt.GetoptError:
            usage()
            sys.exit(2)
        for opt, arg in opts:
            if opt in ("-h", "--help"):
                usage()
                sys.exit()
            elif opt in ("-E", "--event_sim"):
                event_sim = True

        if event_sim:
            set_db_config_file_path(os.path.join(ROOT_DIRECTORY, 'db_adapter_config_event_sim.json'))

        #####################################################
        # Initialize parameters for FLO2D_250 and FLO2D_150 #
        #####################################################

        # source details
        FLO2D_150_v2_params = json.loads(open(os.path.join(ROOT_DIRECTORY, 'res/flo2d_extract_stations/flo2d_150_v2.json')).read())
        FLO2D_model = 'FLO2D'
        FLO2D_150_v2_version = '150_v2'

        # unit details
        unit = 'm'
        unit_type = UnitType.getType('Instantaneous')

        # variable details
        variable = 'WaterLevel'

        # station details
        flo2d_150_v2_grids = read_csv(os.path.join(ROOT_DIRECTORY, 'res/grids/flo2d_150_v2m.csv'))

        pool = get_Pool(host=con_params.CURW_FCST_HOST, port=con_params.CURW_FCST_PORT, user=con_params.CURW_FCST_USERNAME, password=con_params.CURW_FCST_PASSWORD,
                db=con_params.CURW_FCST_DATABASE)

        add_source(pool=pool, model=FLO2D_model, version=FLO2D_150_v2_version, parameters=FLO2D_150_v2_params)
        # add_variable(pool=pool, variable=variable)
        # add_unit(pool=pool, unit=unit, unit_type=unit_type)

        # add flo2d 150 v2 output stations

        channel_cell_map_150_v2 = FLO2D_150_v2_params.get('CHANNEL_CELL_MAP')

        for channel_cell_map_150_v2_key in channel_cell_map_150_v2.keys():
            add_station(pool=pool, name="{}_{}".format(channel_cell_map_150_v2_key, channel_cell_map_150_v2.get(channel_cell_map_150_v2_key)),
                    latitude="%.6f" % float(flo2d_150_v2_grids[int(channel_cell_map_150_v2_key)-1][2]),
                    longitude="%.6f" % float(flo2d_150_v2_grids[int(channel_cell_map_150_v2_key)-1][1]),
                    station_type=StationEnum.FLO2D_150_v2, description="{}_channel_cell_map_element".format(FLO2D_150_V2))

        flood_plain_cell_map_150_v2 = FLO2D_150_v2_params.get('FLOOD_PLAIN_CELL_MAP')

        for flood_plain_cell_map_150_v2_key in flood_plain_cell_map_150_v2.keys():
            add_station(pool=pool, name="{}_{}".format(flood_plain_cell_map_150_v2_key, flood_plain_cell_map_150_v2.get(flood_plain_cell_map_150_v2_key)),
                    latitude="%.6f" % float(flo2d_150_v2_grids[int(flood_plain_cell_map_150_v2_key)-1][2]),
                    longitude="%.6f" % float(flo2d_150_v2_grids[int(flood_plain_cell_map_150_v2_key)-1][1]),
                    station_type=StationEnum.FLO2D_150_v2, description="{}_flood_plain_cell_map_element".format(FLO2D_150_V2))

        destroy_Pool(pool=pool)

    except Exception:
        print("Initialization process failed.")
        traceback.print_exc()
    finally:
        print("Initialization process finished.")

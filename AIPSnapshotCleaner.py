"""
Name: AIPSnapshotCleaner.py

Author: Guru Pai

Date: Thu 05/24/2019 

# TODO
This python script inserts two background facts into AAD.
1. 
2. 

Arguments:
1. 

NOTE:
By design, this script populates the background fact for a single app.

Prerequisites:
1. Before running this script, ensure that you have these two background facts in the application's assessment
   model and have generated a snapshot using that assessment model.
2. The app and the new snapshot must be conslidated in the HD that is being used. Be sure to update the HD URL below.
"""
__version__ = 1.0

import os
import json
import logging
import sys
import time
import urllib.request
import requests
import yaml
from xml.dom import minidom
from datetime import date
from datetime import datetime
from operator import itemgetter
from subprocess import PIPE, STDOUT, DEVNULL, run, CalledProcessError

# Logger settings.
logger = logging.getLogger(__name__)
shandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(filename)s [%(funcName)30s:%(lineno)-4d] %(levelname)-8s - %(message)s')
shandler.setFormatter(formatter)
logger.addHandler(shandler)
logger.setLevel(logging.INFO)

# Global vars
base_url = ''
domain = ''
username = ''
password = ''
CAST_HOME = ''

apps = []
connection_profiles = []
snapshot_info = []

config_settings = {}

delete_snapshots = False

def read_yaml():
    global config_settings

    try:
        with open('resources\\AIPCleaner.yaml') as y_file:
            config_settings = yaml.safe_load(y_file)
    except (FileNotFoundError, IOError) as exc:
        logger.error('An IO exception occurred while opening YAML file. Error: %s' % (exc))
    except yaml.YAMLError as exc:
        logger.error('An exception occurred while reading YAML file. Error: %s' % (exc))
    except:
        logger.error('An unknow exception occurred while reading YAML file.')
    finally:
        logger.info('Setting successfully retireved from the YAML file.')

def read_pmx():
    global connection_profiles

    pmx_file = config_settings['CMS']['pmx_file']

    logger.debug('PMX File:%s' % pmx_file)

    try:
        with minidom.parse(pmx_file) as dom:
            cps = dom.getElementsByTagName('connectionprofiles.ConnectionProfilePostgres')

            for cp in cps:
                name = cp.getAttribute('name')
                schema = cp.getAttribute('schema')
                connection_profiles.append({"name": name, "schema": schema})
    except (IOError) as exc:
        logger.error('Profile names retrival failed.. Aborting.. Error:%s' % (str(exc)))
        raise FileNotFoundError from IOError
    finally:
            logger.debug('Names found: %s' % connection_profiles)

def get_apps():
    global apps

    data = []
    response = ""
    id = ''
    name = ''
    db = ''
    s_href = ''

    # Retrieve names of all apps.
    __headers = {'Accept':'application/json'}

    url =  base_url + '/' + domain + '/applications/'
    auth = (username, password)

    logger.debug('url:%s' % url)

    try:
        # Before making the REST call, ensure that AAD cache is refreshed.
        reload_server_mem_cache()

        with requests.get(url, headers=__headers, auth=auth, stream=True) as response:
            response.raise_for_status()

            if (response.status_code == requests.codes.ok):
                data = response.json()

            for item in data:
                id = item['href'].split('/')[-1]
                db = item['adgDatabase']
                name = item['name']
                s_href = item["snapshots"]['href']

                apps.append({"id": id, "name": name, "adgDatabase": db, "s_href": s_href})

                logger.debug('Found apps:id:%s|name:%s|adgDatabase:%s|s_href:%s' % (id, name, db, s_href))

            return True
    except (requests.HTTPError) as exc:
        logger.error('requests.get failed while retrieving list of applications. Message:%s' % (str(exc)))
        raise
    except:
        logger.error('An exception occurred while retrieving list of applications. Aborting..')
        raise

def reload_server_mem_cache():
    """
    Performs AAD server memory cache. This is needed to ensure that all apps and snapshots
    are visible before making the AAD REST API calls to retireve apps and snapshots.
    
    Returns:
        None - Only throws exception, if one occurs.
    """

    response = ""

    # Retrieve names of all apps.
    __headers = {'Accept':'application/json'}

    url =  base_url + '/server/reload'
    auth = (username, password)

    logger.debug('url:%s' % url)

    try:
        with requests.get(url, headers=__headers, auth=auth, stream=True) as response:
            response.raise_for_status()

            if (response.status_code == requests.codes.ok):
                # Not expecting any data from this call.
                pass
    except (requests.HTTPError) as exc:
        logger.error('requests.get failed while reloading server memory cache. Message:%s' % (str(exc)))
        raise

def get_all_snapshots():
    """
    Retrieves all the snapshots for an app using the snapshot HREF stored in the global 'apps' list.
    Invokes the get_snapshots() function to retrieve all snapshots for the given app.
    """
    global snapshot_info

    # Spin thru the apps and retrieve snapshot href.
    for app in apps:
        snapshots = []

        for key, val in app.items():
            if (key == 's_href'):
                s_href = val

                # Retreive all snapshots for an app, by passing the snapshot URL as the argument.
                snapshots = get_snapshots(s_href)

            for snapshot in snapshots:
                snapshot_info.append(snapshot)

    logger.debug(snapshot_info)

def get_snapshots(s_href):
    snapshots = []
    data = []
    snap_href = ''
    __headers = {'Accept':'application/json'}

    URL = base_url + '/' + s_href
    auth = (username, password)
    response = ""

    #logger.info('Getting a list of snapshots for %s' % application_name)

    try:
        with requests.get(URL, headers=__headers, auth=auth, stream=True) as response:
            response.raise_for_status()

            if (response.status_code == requests.codes.ok):
                data = response.json()

            for item in data:
                # We just need that latest snapshot id, as that is the only thing snapshot 
                # that needs to be updated. The REST call returns that as the first entry.

                snapshot = {}

                snap_href = item["href"]
                app_name = item["name"]
                annot = item["annotation"]
                label = annot["name"]
                # TODO: Save as datetime object?
                snap_dttm = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime((annot['date']['time']) // 1000.0))
                delete_flag = False

                snapshot = {"app_name": app_name, "href": snap_href, "label": label, "snap_dttm": snap_dttm, "delete_flag": delete_flag}
                logger.debug('Found snapshot:%s' % snapshot)
                snapshots.append(snapshot)

    except (requests.HTTPError) as exc:
        logger.error('requests.get failed while retrieving snapshots. Message:%s' % (str(exc)))
        raise
    finally:
        logger.debug(snapshots)
        return snapshots

def mark_snapshots_for_deletion():
    """
    This function marks snapshots that are to be deleted. It does NOT DELETE them.

    Snapshots which need to be deleted are marked for deletion by setting the delete_flag 
    to True, in the snapshot_info list.

    Logic:
    1. Retrieve the snapshot date.
    2. Check which year it belongs to. The script only considers the current year, previous year
       and other years.
    3. Loop thru each snapshot in snapshot_info:
    4.    For each snapshot for an app, based on the retention criteria for the current, previous and other years, 
          call the appropriate funtion to determine if the snapshot is the latest or if there are later snapshots.
    5.    If a newer snapshot exists, mark the current snapshot for deletion UNLESS it meets the 'keep_latest_n_snapshots'
          criteria.
    6. If 'keep_baseline' setting is enabled, call the preserve_baseline_snapshots() function to prevent deletion of
       baseline snapshots.

    TODO:
    By leveraging advanced Python features, we may not have to iterate through each snapshot for a given app.
    """
    global snapshot_info

    temp_app_name = ''
    snapshot_counter = 0

    # Flag snapshots that can be deleted, based on the retention policy.

    curr_year_policy = config_settings['retention_policy']['current_year']
    prev_year_policy = config_settings['retention_policy']['prev_year']
    other_years_policy = config_settings['retention_policy']['other_years']
    keep_baseline = config_settings['retention_policy']['keep_baseline']
    latest_snapshots_to_keep = config_settings['retention_policy']['keep_latest_n_snapshots']

    logger.info('Retention policies - CY:%s|PY:%s|OY:%s|baseline:%s' % (curr_year_policy, prev_year_policy, other_years_policy, keep_baseline))

    for i, elem in enumerate(snapshot_info):
        # Get the date of the snapshot.
        app_name = elem['app_name']
        snap_dttm = datetime.strptime(elem['snap_dttm'], "%Y-%m-%d %H:%M:%S")
        curr_year = date.today().strftime('%Y')
        snap_year = snap_dttm.strftime('%Y')

        # Retain the latest N snapshots, as specified in the YAML file.
        #
        # NOTE: The REST call always returns snapshots in a descending order. 
        #       Leveraging this feature to avoid additional processing.

        snapshot_counter += 1

        if (temp_app_name == ''):
            temp_app_name = app_name
        elif (temp_app_name != app_name):
            # App name changed. Reset count and name.
            snapshot_counter = 1
            temp_app_name = app_name
        else:
            pass

        if (snapshot_counter <= latest_snapshots_to_keep):
            # Keeping the latest set of N snapshots for the app. 
            logger.info('Application:%s|Snapshot:%s| Snapshot will not be deleted as %d latest snapshots are to be kept. Skipping.' % (app_name, snap_dttm, latest_snapshots_to_keep))
            continue

        # NOTE: Assuming that snapshots don't exist for a future date and this is not handled.
        year_diff = int(curr_year) - int(snap_year)

        logger.debug('app_name:%s|curr_year:%s|snap_date:%s|snap_year:%s|year_diff:%s' % (app_name, curr_year, snap_dttm, snap_year, year_diff))

        # Is the snapshot from the current year?
        if (year_diff == 0):
            # Current year
            if (curr_year_policy == 'M'):
                ret_policy = 'Monthly'
            elif (curr_year_policy == 'Q'):
                ret_policy = 'Quarterly'
            elif (curr_year_policy == 'Y'):
                ret_policy = 'Yearly'
        elif (year_diff == 1):
            # Previous year
            if (prev_year_policy == 'M'):
                ret_policy = 'Monthly'
            elif (prev_year_policy == 'Q'):
                ret_policy = 'Quarterly'
            elif (prev_year_policy == 'Y'):
                ret_policy = 'Yearly'
        elif (year_diff > 1):
            # Previous years
            if (other_years_policy == 'M'):
                ret_policy = 'Monthly'
            elif (other_years_policy == 'Q'):
                ret_policy = 'Quarterly'
            elif (other_years_policy == 'Y'):
                ret_policy = 'Yearly'
        else:
            # TODO: 
            # ERROR: Unhandled. 
            pass

        # Mark snapshots for deletion, based on the retention policy.

        if (ret_policy == 'Monthly'):
            logger.debug('Retention policy is Monthly.')
            # Is this the latest snapshot for the month?
            if (is_snapshot_latest_monthly(app_name, snap_dttm) == True):
                # Snapsot NOT TO BE deleted. Nothing to do, as the default 'delete_flag' for the snapshot is False.
                logger.debug('app_name:%s|snap_date:%s - Is the latest snapshot of the month and will NOT BE deleted.' % (app_name, snap_dttm))
            else:
                # Mark the snapshot for deletion
                snapshot_info[i]['delete_flag'] = True

                logger.debug('Marking snapshot %s from application %s for deletion.' % (snap_dttm, app_name))
        elif (ret_policy == 'Quarterly'):
            logger.debug('Retention policy is Quarterly.')
            if (is_snapshot_latest_quarterly(app_name, snap_dttm) == True):
                # Snapsot NOT TO BE deleted. Nothing to do, as the default 'delete_flag' for the snapshot is False.
                logger.debug('app_name:%s|snap_date:%s - Is the latest snapshot of the quarter and will NOT BE deleted.' % (app_name, snap_dttm))
            else:
                # Mark the snapshot for deletion
                snapshot_info[i]['delete_flag'] = True

                logger.debug('Marking snapshot %s from application %s for deletion.' % (snap_dttm, app_name))
        elif (ret_policy == 'Yearly'):
            logger.debug('Retention policy is yearly.')
            if (is_snapshot_latest_yearly(app_name, snap_dttm) == True):
                # Snapsot NOT TO BE deleted. Nothing to do, as the default 'delete_flag' for the snapshot is False.
                logger.debug('app_name:%s|snap_date:%s - Is the latest snapshot of the year and will NOT BE deleted.' % (app_name, snap_dttm))
            else:
                # Mark the snapshot for deletion
                snapshot_info[i]['delete_flag'] = True

                logger.debug('Marking snapshot %s from application %s for deletion.' % (snap_dttm, app_name))

    if (keep_baseline):
        preserve_baseline_snapshots()

def is_snapshot_latest_monthly(application_name, snapshot_datetime):
    is_latest = True # Default to True.

    # For the given app, need to iterate over the snapshots for the current month 
    # and year and determine if the snapshot is the latest for the month.

    snapshot_year = int(snapshot_datetime.strftime('%Y'))
    snapshot_month = int(snapshot_datetime.strftime('%m'))

    logger.debug('Processing - App:%s|snapshot_datetime:%s' % (application_name, snapshot_datetime))

    for snapshot in snapshot_info:
        app_name = snapshot['app_name']

        if (app_name == application_name):
            snap_dttm = datetime.strptime(snapshot['snap_dttm'], "%Y-%m-%d %H:%M:%S")
            snap_year = int(snap_dttm.strftime('%Y'))
            snap_month = int(snap_dttm.strftime('%m'))

            if (snap_year == snapshot_year) and (snap_month == snapshot_month):
                if (snap_dttm < snapshot_datetime):
                    logger.debug('Snapshot %s is < %s. Snapshot is newer.' % (snap_dttm, snapshot_datetime))
                elif (snap_dttm > snapshot_datetime):
                    logger.debug('Snapshot %s is > %s. Newer snapshot found.' % (snap_dttm, snapshot_datetime))
                    is_latest = False
                    break

    return is_latest

def is_snapshot_latest_quarterly(application_name, snapshot_datetime):
    is_latest = False
    num_snapshots_in_qtr = 0

    # For the given app, need to iterate over the snapshots for the current month 
    # and year and determine if the snapshot is the latest for the month.

    snapshot_year = int(snapshot_datetime.strftime('%Y'))
    snapshot_qtr = (int(snapshot_datetime.strftime('%m')) + 2) // 3

    logger.debug('Processing - App:%s|snapshot_datetime:%s|QTR:%d' % (application_name, snapshot_datetime, snapshot_qtr))

    for snapshot in snapshot_info:
        app_name = snapshot['app_name']

        if (app_name == application_name):
            snap_dttm = datetime.strptime(snapshot['snap_dttm'], "%Y-%m-%d %H:%M:%S")
            snap_year = int(snap_dttm.strftime('%Y'))
            snap_qtr = (int(snap_dttm.strftime('%m')) + 2) // 3

            if (snap_year == snapshot_year) and (snap_qtr == snapshot_qtr):
                if (snap_dttm < snapshot_datetime):
                    logger.debug('Snapshot %s is < %s. Snapshot is newer.' % (snap_dttm, snapshot_datetime))
                    is_latest = True
                    num_snapshots_in_qtr += 1
                elif (snap_dttm > snapshot_datetime):
                    logger.debug('Snapshot %s is > %s. Newer snapshot found.' % (snap_dttm, snapshot_datetime))
                    is_latest = False
                    num_snapshots_in_qtr += 1
                    break

    # If only one snapshot exists, mark it as the latest.
    if (num_snapshots_in_qtr == 0):
        is_latest = True

    return is_latest

def is_snapshot_latest_yearly(application_name, snapshot_datetime):
    is_latest = False
    num_snapshots_in_year = 0

    # For the given app, need to iterate over the snapshots for the current month 
    # and year and determine if the snapshot is the latest for the month.

    snapshot_year = int(snapshot_datetime.strftime('%Y'))

    logger.debug('Processing - App:%s|snapshot_datetime:%s' % (application_name, snapshot_datetime))

    for snapshot in snapshot_info:
        app_name = snapshot['app_name']

        if (app_name == application_name):
            snap_dttm = datetime.strptime(snapshot['snap_dttm'], "%Y-%m-%d %H:%M:%S")
            snap_year = int(snap_dttm.strftime('%Y'))

            if (snap_year == snapshot_year):
                if (snap_dttm < snapshot_datetime):
                    logger.debug('Snapshot %s is < %s. Snapshot is newer.' % (snap_dttm, snapshot_datetime))
                    is_latest = True
                    num_snapshots_in_year += 1
                elif (snap_dttm > snapshot_datetime):
                    logger.debug('Snapshot %s is > %s. Newer snapshot found.' % (snap_dttm, snapshot_datetime))
                    is_latest = False
                    num_snapshots_in_year += 1
                    break

    # If only one snapshot exists, mark it as the latest.
    if (num_snapshots_in_year == 0):
        is_latest = True

    return is_latest

def preserve_baseline_snapshots():
    global snapshot_info

    min_snap_dttm = ''
    temp_app_name = ''

    # For the given app, need to iterate over the snapshots for the current month 
    # and year and determine if the snapshot is the latest for the month.

    for i, elem in enumerate(snapshot_info):
        app_name = elem['app_name']
        snap_dttm = elem['snap_dttm']

        app_dict = [x['snap_dttm'] for x in snapshot_info if x['app_name'] == app_name]
        #print(app_dict)
        
        min_snap_dttm = min(app_dict)
        #print('min_snap_dttm:%s' % min_snap_dttm)

        if (temp_app_name == '') or (temp_app_name != app_name):
            temp_app_name = app_name

        if (elem['app_name'] == temp_app_name) and (elem['snap_dttm'] == min_snap_dttm):
            snapshot_info[i]['delete_flag'] = False

    return

def drop_snapshots():
    # NOTE:
    # Snapshots will NOT BE DELETED unless the argument 'drop-snapshots' is 
    # passed in. Otherwise, the program only displays the list of snapshots
    # that have been marked for deletion but no snapshots will be deleted.
    prev_app_name = ''
    snapshots_to_drop = []
    cli_command = []

    for snapshot in snapshot_info:
        # Get the central schema name from the apps list, by searching the app name.
        app_name = snapshot['app_name']
        adg_db = next(item for item in apps if item["name"] == app_name)['adgDatabase']

        # Get the connection profile name from the connection_profiles list, using the 
        # mngt schema name.
        mngt_name = adg_db.replace('_central', '_mngt')
        profile_name = next(item for item in connection_profiles if item["schema"] == mngt_name)['name']

        if (prev_app_name == ''):
            prev_app_name = app_name
        elif (app_name != prev_app_name):
            # Invoke the CLI to drop the snapshots for the prior app.

            if (len(snapshots_to_drop) != 0):
                cli_command.append(','.join(snapshots_to_drop))

                try:
                    exec_cli(cli_command)
                except:
                    logger.info('CLI:%s' % cli_command)
                    logger.error('An error occurred while deleting snapshots.')

            snapshots_to_drop = []
            prev_app_name = app_name

        snap_dttm = datetime.strptime(snapshot['snap_dttm'], "%Y-%m-%d %H:%M:%S")
        delete_flag = snapshot['delete_flag']
        capture_date = snap_dttm.strftime("%Y%m%d%H%M")

        logger.debug('App:%s | Snapshot:%s | delete_flag:%s' % (app_name, snap_dttm, delete_flag))

        if (delete_flag):
            if (delete_snapshots):
                logger.info('App:%s | Snapshot date:%s | This snapshot will be dropped' % (app_name, snap_dttm))

                snapshots_to_drop.append(capture_date)

                # Create the CLI only once.
                if (len(snapshots_to_drop) == 1):
                    cli_command = [ \
                            f'"{CAST_HOME}\\cast-ms-cli.exe" DeleteSnapshotsInList -connectionProfile' \
                            f' "{profile_name}" -appli {app_name} -dashboardService {adg_db} -snapshots ' \
                        ]

                logger.debug('CLI Command:%s' % (cli_command))
            else:
                logger.info('App:%s | Snapshot date:%s | Snapshot marked for deletion, but -drop arg not supplied. NOT deleting.' % (app_name, snap_dttm))
        else:
            logger.info('App:%s | Snapshot date:%s | Not marked for deletion. Keeping this snapshot.' % (app_name, snap_dttm))

    # Applicable only in the case of the last app in the loop.
    #
    # Invoke the CLI to drop the snapshots for it.

    if (len(snapshots_to_drop) != 0):
        cli_command.append(','.join(snapshots_to_drop))

        try:
            exec_cli(cli_command)
        except:
            logger.info('CLI:%s' % cli_command)
            logger.error('An error occurred while deleting snapshots.')

def exec_cli(cli):
    cli_str = ''.join(cli)
    try:
        logger.info('Calling CLI:%s' % cli_str)

        cli_cmd=run(cli_str, stdout=PIPE, stderr=STDOUT, shell=True, check=True)

        logger.debug('returncode:%s' % cli_cmd.returncode)
        logger.debug('stdout:%s' % cli_cmd.stdout)
        logger.debug('stderr:%s' % cli_cmd.stderr)

        cli_cmd.check_returncode()

        if (cli_cmd.returncode == 0):
            logger.info('Marked snapshots successfully deleted.')

    except CalledProcessError as exc:
        logger.error('An error occurred while executing CLI:%d. CLI:%s' % (exc.returncode, exc.cmd))
        raise

def main():
    global base_url, domain, username, password, CAST_HOME
    try:
        # Read the YAML file to get the config settings.
        read_yaml()

        # Set some global vars
        base_url = config_settings['Dashboard']['URL']
        domain = config_settings['Dashboard']['domain']
        username = config_settings['Dashboard']['username']
        password = config_settings['Dashboard']['password']
        CAST_HOME = config_settings['other_settings']['cast_home']

        # Setup logging to file
        log_file = config_settings['other_settings']['log_folder'] + '\\AIPSnapshotCleaner' + time.strftime('%Y%m%d%H%M%S') + '.log'
        fhandler = logging.FileHandler(log_file, 'w')
        fhandler.setFormatter(formatter)
        logger.addHandler(fhandler)

        # Read the CAST-MS conection profile file to retrieve profile names.
        read_pmx()
        # Grab names of all apps from the dashboard via REST call.
        get_apps()
        # Retireve a list of snapshots from the dashboard via REST call.
        get_all_snapshots()
        # Indentify snapshots that can be deleted.
        mark_snapshots_for_deletion()
        # Delete snapshots that are marked for deletion.
        drop_snapshots()

    except:
        logger.error('Aborting due to a prior exception. See the log file for complete details.')
        sys.exit(6)
    else:
        pass

# Start here
# TODO: Need an argument to indicate if snapshot need to be deleted.

if __name__ == "__main__":
    logger.info('Starting process')
    args = sys.argv[1:]

    count = len(args)

    if (count == 1):
        delete_flag = args.__getitem__(0)
        print(delete_flag)
        if (args[0] == '-drop'):
            logger.info('The -drop argurment activated. Snapshots will be dropped.')
            delete_snapshots = True
        else:
            logger.warn('The %s argurment is unknown. Ignoring it.' % args[0])
    elif (count > 1):
        logger.error('Invalid number of arguments passed, when expecting one. Aborting.')
        sys.exit(1)
    else:
        # Zero args passed. Which means that snapshots should not be deleted.
        # Snasphots marked for deletion will only be listed for INFO ONLY.
        logger.info('The -drop argurment was not passed in. Snapshots will not be dropped.')
        logger.info('To drop sanpshots, invoke snasphot clearner with a -drop argument.')

    main()

sys.exit(0)
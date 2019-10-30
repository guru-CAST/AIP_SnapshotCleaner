# AIP Snapshot Cleaner

This script can be used to cleanup older snapshots in CAST AIP. Snapshots are cleaned up based on  the retention criteria specified in
the resources/AIPCleaner.yaml file.

## Prerequisites
The script is written in Python. Ensure that you have Python __3.6__ or above installed.
It uses the following Python packages. Ensure these are installed using the Python __PIP__ command. PIP installs dependent packages, where needed.

From this project root directory:

`pip install -r requirements.txt`

Refer to [Installing Python Modules](https://docs.python.org/3.6/installing/) webpage for more information on how to install Python modules.

## First time setup
1. Copy the files and folder into a folder named AIP_SnapshotCleaner.
2. Create a blank folder named __logs__ in the newly created folder.
3. Edit the ``AIPCleaner.YAML`` file in the resources file and setup the retention criteria as needed. See for details.

## Snapshot retention criteria and other settings:
At this time, the script support the following retention criterias:
- __M__ - Retain monthly snapshots
- __Q__ - Retain quarterly snapshots
- __Y__ - Retain yearly snapshots

Setup the retention criteria as follows, for example, to retain monthly snapshots for the current year, quarterly snapshots for the prior year and yearly snapshots for the rest of the years.

```
retention_policy:
  current_year: M
  prev_year: Q
  other_years: Y

Use the following option to ensure that the latest N number of snapshots are always kept:
  keep_latest_n_snapshots: 5

Additionally, use the following option to retain baseline snapshots:
  keep_baseline: true
```

## Other settings
Update the AAD URL, credentials, name and location of the CAST-MS PMX file, log folder location and CAST installation folder location.

## Before running cleanup
The script deletes the snapshots that match the criteria specified, using the __CAST-MS CLI__ command. This causes snapshots to be dropped from the dashboard service schema for each of the apps. So before running cleanup each time, ensure that the dashboard service schema is backed up.

## How to run the cleanup script
The script can by running by invoking python and passing in the name of the script as an argument. Before starting, change directoy to move to the script folder.

``python AIPSnapshotCleaner.py`` - This will invoke the script in a display only mode, where it does not delete snapshots, but will display which snapshots will be dropped. To drop outdated snapshots, invoke the script with a __-drop__ argument as in: ``python AIPSnapshotCleaner.py -drop``

:point_right: __NOTE:__ Ensure that the id being used for connecting to AAD (the _username_ specified in the __dashboard__ section in the YAML file) has __admin__ rights to the HD dashboard. Else, some actions will fail.

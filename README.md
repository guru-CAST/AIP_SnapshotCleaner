<strong>AIP_SnapshotCleaner</strong>

This script can be used to cleanup older snapshots in CAST AIP. Snapshots are cleaned up based on  the retention criteria specified in 
the resources/AIPCleaner.yaml file.

<strong>Prerequisites</strong>:<br>
The script is written in Python. Ensure that you have Python <b>3.6</b> or above installed. 
It uses the following Python packages. Ensure these are installed using the Python PIP command. PIP installs dependent packages, where needed.

<li>requests</li>
<li>PyYAML</li><br>

<strong>First time setup</strong>:<br>
<li>Copy the files and folder into a folder named AIP_SnapshotCleaner.</li> 
<li>Create a blank folder named <B>logs</B> in the newly created folder.</li>
<li>Edit the AIPCleaner.YAML file in the resources file and setup the retention criteria as needed. See for details.</li><br>

<strong>Snapshot retention criteria and other settings</strong>:<br>
At this time, the script support the following retention criterias:
<li>M - Retain monthly snapshots</li>
<li>Q - Retain quarterly snapshots</li>
<li>Y - Retain yearly snapshots</li>
You setup the criteria as follows, for example, which will retain monthly snapshots for the current year, quarterly snapshots for the prior year and yearly snapshots for the rest of the years.

retention_policy:
  current_year: M
  prev_year: Q
  other_years: Y

Use the following option to ensure that the latest N number of snapshots are always kept.
  keep_latest_n_snapshots: 5
  
Additionally, use the following option to retain baseline snapshots:
  keep_baseline: true
  
Other settings:
Update the AAD URL, credentials, name and location of the CAST-MS PMX file, log folder location and CAST installation folder location. 

<strong>Before running cleanup</strong>:<br>
The script deletes the snapshots that match the criteria specified, using the <b>CAST-MS CLI</b> command. This causes snapshots to be dropped from the dashboard service schema for each of the apps. So before running cleanup each time, ensure that the dashboard service schema is backed up. 

<strong>How to run the cleanup script</strong>:
The script can by running by invoking python and passing in the name of the script as an argument. Before starting, change directoy to move to the script folder. 

python AIPSnapshotCleaner.py - This will invoke the script in a display only mode, where it does not delete snapshots, but will display which snapshots will be dropped. To drop outdated snapshots, invoke the script with a <b>-drop</B> argument as in: python AIPSnapshotCleaner.py -drop



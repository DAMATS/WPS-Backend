# WPS-Backend

This package implements simple WPS asynchronous back-end 
[EOxServer](https://github.com/EOxServer/eoxserver) component.

## Overview
The EOxServer WPS-Backend is a simple Python package allowing asynchronous execution
of the EOxServer's WPS jobs.

The package consist of two parts:
 * simple daeom employing a simple FIFO queue and a pool of processes to execute the WPS asynchrounous jobs;
 * EOxServer component implementing the `AsyncBackendInterface` interface.
These two part communnicate via a private IPC socket. 

The daemon shares the full Django context witht the configured EOxServer instance and therefore each EOxServer instance requires its own instance of the WPS-backend daemon.

## Installation

Use the usual `setup.py` (Python `setuptools`).

_More details TBD_

## Configuration

The damon is configured via the EOxServer configuration file `eoxserver.conf` in the `service.ows.wps` section:

```
[services.ows.wps]

# mandatory directory of the temporary WPS workspace
path_temp=<path_temp>

# mandatory directory of the permanent WPS storage (published outputs)
path_perm=<path_perm>

# mandatory directory of the taks persistend storage
path_task=<path_task> 

# URL base mapped to the permanent WPS storage
url_base=http://<hostname>/wps

# IPC socket file
socket_file=<socket_file>

# max. number of the queued jobs before the jobs get refused
max_queued_jobs=256

# max. number of parallel processes employed by the pool
num_workers=4
```

In addition, the permanent WPS storage directory has to be mapped to WPS base URL to allow the outputs to be visible. The Apache server configuration may, e.g., contain following line:

```
Alias /wps <path_perm>
```


## Execution

The daemon can started by the follwing command:
```
python -EsOm eoxs_wps_async.daemon <settings-module> [<python-path> ...]
```
The daemon requires the setting module of the EOxServer Django instance and the optional one or more Python paths if neccessary.

It is recommended that the daemon is intergared as a system service. This can be done, e.g., by the following `systmed` servive configuration file (on a `systemd` enabled GNU/Linux system):
```
[Unit]
Description= Asynchronous EOxServer WPS Daemon
After=network.target
Before=httpd.service

[Service]
Type=simple
User=damats
ExecStart=/usr/bin/python -EsOm eoxs_wps_async.daemon <instance>.settings <instance-root>/<instance>

[Install]
WantedBy=multi-user.target
```
Replace `<instance>` with the EOxServer instance name and the `<instace-root>` by the path where this instance is located.

Save the `systemd` service file, e.g., to `/etc/systemd/system/eoxs_wps_async.service` and execute following commands to start the service (with the right permissions):
```
systemctl daemon-reload
systemctl enable eoxs_wps_async.service
systemctl start eoxs_wps_async.service
```

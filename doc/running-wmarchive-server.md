# Running the WMArchive Server

Follow this procedure to run the WMArchive Server on your local machine or on a remote node.

**On your local machine:**

- Install MongoDB (e.g. `brew install mongo` on macOS)
- Install `python2.7` and the following packages, e.g. in a `virtualenv`:

	```sh
	> pip list
	avro (1.8.1)
	bz2file (0.98)
	Cheetah (2.4.4)
	CherryPy (6.0.2)
	pymongo (3.2.2)
	```

**On a remote node:**

- Setup the WMArchive environment by adding the following to `~/.bash_profile`:

	```
	source /data/srv/current/apps/wmarchive/etc/profile.d/init.sh
	export JAVA_HOME=/usr/lib/jvm/java

	# Common python packages
	export PYTHONPATH=$PYTHONPATH:/data/wma/usr/lib/python2.7/site-packages

	# Spark setup
	export SPARK_HOME=/usr/lib/spark
	export SPARK_DIST_CLASSPATH=$(hadoop classpath)

  # Local WMArchive python packages
  export PYTHONPATH=/afs/cern.ch/user/n/USERNAME/WMArchive/src/python:$PYTHONPATH
	```

	Make sure to replace ´USERNAME`.

**Both:**

- Clone [WMArchive](https://github.com/dmwm/WMArchive) (or your fork) and [WMCore](https://github.com/dmwm/WMCore).
- Optionally copy the `WMArchive/etc/wmarch_config_local.py.example` to `WMArchive/etc/wmarch_config_local.py` to modify configurations. On a remote machine that runs a WMArchive server already, make sure to change the port.
- Adapt the [`./scripts/run_server.sh`](scripts/run_server.sh) and [`./scripts/stop_server.sh`](scripts/stop_server.sh) scripts to your system and copy/symlink them to the WMArchive directory. Make sure to use the `wmarch_config_local.py` configuration file if necessary.
- Run `./run_server.sh` from the WMArchive directory.

	The script will stop any running server, possibly copy the `./src/{css, images, js, maps, templates}/` directories to `./data/` and then run the server. You will find the log output in the log file you provided in `run_server.sh`.

**To query the remote server:**

- From your local machine setup an SSH tunnel:
	```
	ssh -L 8888:localhost:REMOTE_WMARCHIVE_PORT USERNAME@vocms013
	```
- Open [http://localhost:8888/wmarchive/web/performance](http://localhost:8888/wmarchive/web/performance) in any browser.

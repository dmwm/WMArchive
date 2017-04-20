# Generating sample data

Follow this procedure to generate sample data from the HDFS and import them to your local MongoDB instance for the WMArchive server to display them.

- First obtain a Kerberos token and SSH into the `vocms013` node for access to the HDFS:

  ```
  kinit # Obtain a Kerberos token
  klist # Check current tokens
  ssh USERNAME@vocms013
```
- `~` is your mounted AFS user directory. Follow the procedure in [Running the WMArchive Server](./running-wmarchive-server.md) to setup the WMArchive environment here.
- Use one if the scripts detailed in the [Aggregation procedure](./aggregation-procedure.md) documentation to run the aggregation.

	Keep in mind the script will store its result in MongoDB unless you ensured otherwise. It also produces a JSON file with its output.

---

- Your can now copy the JSON file to your local machine:

	```
	scp USERNAME@vocms013:~/WMArchive/RecordAggregator_result.json .
	```

- Start MongoDB on the port used by WMArchive (see `wmarch_config[_local].py`)

	```
	mongod --port 8230 --dbpath ./data/db
	```

- Import the sample data to the `aggregated` database and `performance` collection:

	```
	mongoimport --port 8230 --db aggregated --collection performance RecordAggregator_result.json
	```
- You can at any time open the MongoDB shell to check the contents of the database:

	```
	mongo --port 8230
	> help # to show useful commands
	> use aggregated # switch to the aggregated metrics database
	> show collections
	```
	The WMArchive server reads performance data from the `aggregated.performance` collection.

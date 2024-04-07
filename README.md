# Instructions related to the project and how to run the code

We tried to simplify the code as much as possible, so that it is easy to understand and follow. The code is divided into 3 main parts:
### run.py
To ensure that the process is automated and that the user does not have to do many things manually, we created a functionality which creates an ssh connection to the VM provided by the UPC, starts the HDFS database and it also closes HDFS and the ssh connection after all other operations have been performed.
To run the whole process, you would have to run 'python src/run.py --help' and 'python src/run.py retrive --help' to get the list of arguments for the command line and choose the desired configuration for data ingestion.
As an example, for ingesting data from all sources and all the versions you would run: 'python src/run.py retrive --host 10.4.41.55 --source * --all'

### collector.py

### loader.py
The loader has the purpose to move the data from temporal to persistent. This is basically the Data Persistence Loader.
It creates a connection to the specified Mongo Client, DB, and Collection, and then it inserts the data into the collection. This process is automated.
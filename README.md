# Instructions related to the project and how to run the code

We tried to simplify the code as much as possible, so that it is easy to understand and follow. The code is divided into 3 main parts:

## run.py

This is the entry point to run the whole process, you can run `python src/run.py --help` and `python src/run.py retrive --help` to get the list of arguments for the command line and choose the desired configuration for data ingestion.

## service.py

To ensure that the process is automated and that the user does not have to do many things manually, we created a functionality which creates an ssh connection to the VM provided by the UPC, starts the HDFS and MongoDB services and it also closes them at the end of execution.

## landing/

We grouped all files related to the landign zone (temporal ans persistent) in the same folder.

### collector.py

The collector has the purpose of going to the source (a local folder or a web page) and retrive the dataset to temporal landing in HDFS.
It makes use of the metadata to retrive from the source described there.
This process is automated.

### loader.py

The loader has the purpose to move the data from temporal to persistent.
This is basically the Data Persistence Loader.
It creates a connection to the specified Mongo Client, DB, and Collection, and then it inserts the data into the collection.
This process is automated.

### metadata/

A collection of json file with the necessary information to load a dataset from a specific source.
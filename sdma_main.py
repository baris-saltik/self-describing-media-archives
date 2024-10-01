import logging.config, sys, os, threading, queue, math, pprint, shutil, re
import os, pathlib, datetime, logging, time, copy

# Step 1 ################## Read main config and update logging config #################
# Set the logging.conf file first before other modules loads it.
from modules.main_config.main_config import Config
from modules.log_config.log_config import LoggingConf
from modules.vision_ai.vision_ai import VisionAI
from modules.starburst_data.starburst_data import StarburstData
from modules.galaxy_api.galaxy_api import GalaxyAPI


configObj = Config()
configObj.update_main_conf()
configObj.set_defaults()
configObj.initialize_logging_conf()
configObj.update_logging_conf()
config = configObj.mainConfigDict

# Initialize logging
loggingConfig = LoggingConf().config
logging.config.dictConfig(loggingConfig)
logger = logging.getLogger('sdma_main')

# Regular expression for detecting threads
rexThread = re.compile(r"thread\d+")

# Step 1 ################## Load the rest of the custom modules #################
# Load the custom modules which relies on logging configuration
from modules.s3.s3 import S3
from modules.exif.exif import Exif


# Update progress page
def update_progress_page(progressPage = None, progress = -1):
    if progressPage:
        try:
            with open(file = progressPage, mode="w") as file:
                file.write(str(progress))
            logger.debug(f"Updated the progress page with {progress}")
        except Exception as err:
            logger.error(f"Could not update progress page for {progress}")
            logger.error(err)
    else:
        logger.error("No progress page to update!")

# Reset progress
_progress = 200
_progressPage = config['runTime']['progressPage']
update_progress_page(progressPage=_progressPage, progress=_progress)

# Worker thread tasks
def worker(**kwargs):

    # Worker step 1 ################## Create thread's temporary download directory #################

    name = kwargs['name']
    threadPath = kwargs['threadPath']
    threadQueue = kwargs['threadQueue']
    extensions = kwargs['extensions']
    partialDownload = kwargs['partialDownload']
    downloadRange = kwargs['downloadRange']
    incremental = kwargs['incremental']
    extractExif = kwargs['extractExif']
    extractLabels = kwargs['extractLabels']
    config = kwargs['config']
    sendToStarburst = False if config['runTime']['dryRun'] else config['runTime']['sendToStarburst'] 

    updateStatisticsAfter = kwargs['updateStatisticsAfter']

    logger.info(f"Thread {name} has started...")

    try:
        shutil.rmtree(threadPath)
        logger.info(f"Cleared {threadPath} for thread '{name}'")
    except Exception as err:
        logger.info(f"{threadPath} does not exist or could not be cleared")

    try:
        os.makedirs(threadPath)
        logger.info(f"{threadPath} for {name} has been created for thread '{name}'.")
    except Exception as err:
        logger.critical(f"Could not create path '{threadPath}' for thread '{name}'. {name} is quiting...")

    # Worker step 2 ################## Intialize s3 client for the thread #################

    logger.info(f"Initializing s3 client for thread {name}...")
    s3 = S3(config = config)
    s3.create_client()
    s3.verify_bucket()
    
    # Worker step 3 ################## Downlaod objects ti thread's temporary directory #################
    # Get the list of objects to process from thread's assigned page
    for _page in kwargs['pages']:
        processingDict = {}
        listOfObjects = []

        for item in _page['Contents']:
            listOfObjects.append(item['Key'])

        # print(f"{kwargs['name']}: {objectsToDownload}")
        

        # Filter objects by extension and download filtered object list
        processingDict = s3.download_objects(listOfObjects = listOfObjects, extensions = extensions, incremental = incremental, partialDownload = partialDownload, downloadRange = downloadRange, tempPath = threadPath)

        
        # processingDict is a dictionary of dictionaries. It has the following structure at this point:
        # processingDict = {filePath: {'key': key, 'processed': False}}
        # filePath is a full temp file path, i.e. "C:\temp\thread1\file_1".
        # key is the object key, i.e. "folder1\folder2\folder3\object1".
        # logger.debug(f"Processing list for thread {name}: {processingDict}")


        # Worker step 4 ################## Extract exif data #################
        if extractExif:
            logger.info(f"Thread {name} is starting to extract exif data...")
            exifDataList = None
            exif = Exif(config = config)
            # exifDataList is a list of dictionaries. Each dictionary contains file path, name and exif data.
            exifDataList = exif.extract(path = threadPath + r"\*")
            exif.__exit__()
        
            # pprint.pprint(exifDataList)

            # Create a dictionary with a path as a key out of exifDataList
            if exifDataList:
                exifDataDict = {}
                for dictItem in exifDataList:
                    filePath = os.path.abspath(dictItem['SourceFile'])
                    exifDataDict[filePath] = dictItem
                    # Remove 'SourceFile' key with its value, because it's string value of that has become the key for each item in this dict already
                    exifDataDict[filePath].pop('SourceFile')
            
                # Merge exifDataDict with processingDict
                logger.debug(f"Thread {name} is adding exif data to processing dictionary...")
                for path in processingDict.keys():
                    processingDict[path]['exif'] = exifDataDict[path]

                # At this point processingDict has the following structure:
                # processingDict = {filePath: {'key': key, 'processed': False, 'exif': exif}
                # filePath is a full temp file path, i.e. "C:\temp\thread1\file_1".
                # key is the object key, i.e. "folder1\folder2\folder3\object1".
                # exif is dictionary of exif data key/value pairs.

                # pprint.pprint(processingDict, indent=1)
            else:
                logger.warning(f"Thread {name} could not find any files to extract exif data from...")


        # Worker step 5 ################## Label images #################
        if extractLabels:
            logger.info(f"Started labeling files...")
            
            labeler = VisionAI(config = config)
            # Create a list of file paths out of processingDict to send to vision_ai module
            fileList = list(processingDict)
            labeledFiles = labeler.get_labels(fileList = fileList)
            labeler.__exit__()

            # print(labeledFiles)

            for path in processingDict.keys():
                processingDict[path]['labels'] = labeledFiles[path]
            # At this point processingDict has the following structure:
            # processingDict = {filePath: {'key': key, 'processed': False, 'exif': exif, 'labels': labelDescriptions}
            # filePath is a full temp file path, i.e. "C:\temp\thread1\file_1".
            # key is the object key, i.e. "folder1\folder2\folder3\object1".
            # exif is dictionary of exif data key/value pairs.
            # labelDescriptions is a list, i.e. ["Snow", "Winter Sports", "World"]

        # Worker step 6 ################## Update object metadata #################
        logger.info(f"Updating object metadata...")

        metadata = None
        metadata = s3.update_metadata(processingDict = processingDict, threadQueue = threadQueue, updateStatisticsAfter = updateStatisticsAfter)
        # pprint.pprint(metadata)

        if metadata and sendToStarburst:

            starburst = StarburstData(config = config)
            starburst.create_session()
            if starburst.clusterAvailable:
                logger.info(f"Starburst/DDAE Cluster is active")
                starburst.verify_schema()

                if starburst.schemaExists:
                    starburst.verify_table()
                    if starburst.tableExists:
                        starburst.insert_into_table(metadata = metadata)
                    else:
                        logger.error(f"Table {starburst.table} does not exist!")
                else:
                    logger.error(f"Schema {starburst.schema} does not exist!")
            else:
                logger.error(f"Starburst/DDAE Cluster is not active!")

        # Clean up temporary threadPath
        try:
            shutil.rmtree(threadPath)
            logger.info(f"Cleared the path '{threadPath}' for thread '{name}'")
        except Exception as err:
            logger.info(f"{threadPath} does not exist or could not be cleared")
        
        logger.info(f"Thread {name} has ended it's processing.")

# Main object
class Main(object):

    # Step 1 ###################### Initialize Main ##############
    def __init__(self, config = config):
        logger.info("##########" + " Started execution " + "###########")
        self.initializationFailed = False
        self.parametersSetFailed = False
        self.progress = -1
        self.progressPage = config['runTime']['progressPage']
        update_progress_page(progressPage=self.progressPage, progress=self.progress)

        if not config:
            logger.error("No config provided. Quiting...")
            self.initializationFailed = True
            self.parametersSetFailed = True
        else:
            self.config = config
            logger.setLevel(config['logging']['level'])
    
        if not self.initializationFailed:
            ### Execution vide variables
            # For logging
            self.starburstConfig = config['starburst']

            self.extensions = config['exif']['extensions']
            self.incremental = config['runTime']['incremental']
            self.updateStatisticsAfter = config['runTime']['updateStatisticsAfter']
            self.maxNumberOfThreads = config['runTime']['maxNumberOfThreads']
            self.intervalToCheckThreads = config['runTime']['intervalToCheckThreads']
            self.extractExif = config['runTime']['extractExif']
            self.extractLabels = config['runTime']['extractLabels']
            self.downloadRange = config['runTime']['downloadRange']
            self.sendToStarburst = config['runTime']['sendToStarburst']
            self.schema = config['starburst']['schema']
            self.table = config['starburst']['table']
            self.starburstType = config['starburst']['type'].lower()
            self.locationPrivGranted = False
            self.threadsLaunched = False
            self.threads = {}

            self.partialDownload = False if self.extractLabels else config['runTime']['partialDownload']

            # Execution wide variables
            self.numberOfObjects = 0
            self.numberOfObjectsProcessed = 0
            self.numberOfObjectsSkipped = 0
            self.numberOfThreads = 0

        # Check if the execution is meaningful
        if not self.initializationFailed:
            if not self.extractExif and not self.extractLabels:
                logger.warning(f"Both exif and label processings are disabled. Quiting...")
                logger.info("##########" + " Ended execution " + "###########")
                self.initializationFailed = True

    # initialize
    def initialize(self):
    
        if not self.initializationFailed:
            ################### Create/recreate base temporary download directory #################
            logger.info("Creating/recreating base temporary download directory...")
            tempPath = self.config['runTime']['tempPath']
            tempFullPath = os.path.join(pathlib.Path(__file__).resolve().absolute().parents[0], tempPath)

            try:
                if os.path.exists(path = tempFullPath) and os.path.isdir(tempFullPath):
                    shutil.rmtree(path = tempFullPath)
                os.makedirs(name = tempFullPath)
                logger.info(f"{tempFullPath} has been created as a base temporary download directory.")
                self.tempFullPath = tempFullPath
            except Exception as err:
                logger.critical(f"Could not create/recreate {tempFullPath}, aborting...")
                self.initializationFailed = True
                update_progress_page(progressPage=self.progressPage, progress="init-failed")

        ################### Calculate object counts and pagination #################
        if not self.initializationFailed:
            logger.info(f"Initializing threads...")
            try:
                self.s3 = S3(config = self.config)
                self.s3.create_client()
                self.s3.verify_bucket()
                self.s3.get_count_of_objects_and_paginators()
                self.numberOfObjects = self.s3.numOfObjects
            except Exception as err:
                self.numberOfObjects = 0
                self.initializationFailed = True
                update_progress_page(progressPage=self.progressPage, progress="init-failed")
        
    # Step 2 ###################### Verify Starburst/DDAE Cluster availability ##############
    def check_starburst_connectivity(self):

        try:
            self.starburst = StarburstData(config = self.config)
            self.starburst.create_session()
            if self.starburst.clusterAvailable:
                logger.info(f"Starburst/DDAE Cluster is active")
            else:
                logger.error(f"Starburst/DDAE Cluster is not active!")
                update_progress_page(progressPage=self.progressPage, progress="starburst-failed")
        except Exception as err:
            logger.error("Could not create a connection to Starburst/DDAE")
            update_progress_page(progressPage=self.progressPage, progress="starburst-failed")

    # Step 3 ###################### Create or verify Sarburst/DDAE schema and the table as necessary ##############
    def create_schema_table(self):

        if self.numberOfObjects and self.sendToStarburst:
            try:
                self.starburst = StarburstData(config = self.config)
                self.starburst.create_session()

                if self.starburst.clusterAvailable:
                    logger.info(f"Starburst/DDAE Cluster is active")
                else:
                    logger.error(f"Starburst/DDAE Cluster is not active!")
            except Exception as err:
                logger.error("Starburst/DDAE cluster is not available. Skipping Iceberg operations...")
                self.sendToStarburst = False
                return

        else:
            self.sendToStarburst = False
            return

        if self.starburst.clusterAvailable:
            self.starburst.verify_schema()

        if self.incremental:
            # Incremental run
            if self.starburst.schemaExists:
                self.starburst.verify_table()
                if self.starburst.tableExists:
                    logger.info(f"Table {self.table} exist.")
                else:
                    self.starburst.__exit__()
                    logger.critical(f"Table {self.table} does not exist!")
            # Full run
        else:
            # Set the correct platform API
            if self.starburstType == "galaxy":
                starburstAPI = GalaxyAPI(config = self.config)
            
            starburstAPI.create_session()
            if starburstAPI.sessionCreated:
                starburstAPI.grant_location_privilege()
                if starburstAPI.locationPrivGranted:
                    logger.info(f"Location privileges for {self.starburstConfig['bucket']} granted to {self.starburstConfig['roleName']}")
                else:
                    logger.error(f"Location privileges for {self.starburstConfig['bucket']} could not be granted to {self.starburstConfig['roleName']}!")
            starburstAPI.__exit__()

            if not self.starburst.schemaExists:
                self.starburst.create_schema()
                if self.starburst.schemaExists:
                    logger.info(f"Schema {self.schema} has been created.")
            
            if self.starburst.schemaExists and starburstAPI.locationPrivGranted:
                self.starburst.create_table()
                if self.starburst.tableExists:
                    logger.info(f"Table {self.table} has been created.")
                else:
                    logger.critical(f"Table {self.table} could not be created!.")

        self.starburst.__exit__()
                
        # Modify sendToStartburst if necessary
        self.sendToStarburst = True if self.starburst.tableExists else False

    # Step 4 ###################### Initialize threads ##############
    def initialize_threads(self):

        # Recreate the S3 client, and get the values again for objects might have been added/deleted since the initial state
        try:
            self.s3.create_client()
            self.s3.verify_bucket()
            self.s3.get_count_of_objects_and_paginators()

            self.numberOfObjects = self.s3.numOfObjects
        except Exception as err:
            logger.error(f"S3 connection failed.")
            update_progress_page(progressPage=self.progressPage, progress="init-threads-failed")
            return False

        # Calculate number of pages per thread
        if self.s3.numOfObjects:
            logger.debug(f"########## Max Number of threads: {self.maxNumberOfThreads}")
            if math.floor(self.s3.numOfPages / self.maxNumberOfThreads):
                self.numberOfThreads = self.maxNumberOfThreads
                pagesPerThread = math.floor(self.s3.numOfPages / self.maxNumberOfThreads)
                
            else:
                self.numberOfThreads = self.s3.numOfPages
                pagesPerThread = 1
            
            # Populate threads dictionary with thread details
            for i in range(1,self.numberOfThreads+1,1):
                self.threads['thread' + str(i)] = {'name': 'thread' + str(i), 'numberOfPages': pagesPerThread, 'pages': [], 'threadQueue': queue.Queue(), 'threadPath': os.path.join(self.tempFullPath, 'thread' + str(i))} 
            
            # If there are more pages than number of threads, distribute excessive pages across threads
            if self.s3.numOfPages % self.numberOfThreads:
                for i in range(1, (self.s3.numOfPages % self.numberOfThreads) + 1, 1):
                    self.threads['thread' + str(i)]['numberOfPages'] += 1
        else:
            logger.warning(f"No objects have been found! Quiting...")
            sys.exit()

        # Log thread stats
        logger.info(f"Number of maximum threads: {self.maxNumberOfThreads}")
        logger.info(f"Number of actual threads: {self.numberOfThreads}")

        # Assign pages to the threads
        logger.debug(f"Assigning actual pages to threads...")
        self.s3.assign_pages_to_threads(threads = self.threads)
        self.s3.__exit__()

        # Instantiate the actual threads
        logger.info(f"Launching a worker per thread...")

        for tId in self.threads.keys():
            kwargs = {'name': self.threads[tId]['name'],
                'numberOfPages': self.threads[tId]['numberOfPages'], 
                'pages': self.threads[tId]['pages'], 
                'threadQueue': self.threads[tId]['threadQueue'],
                'threadPath': self.threads[tId]['threadPath'],
                'extensions': self.extensions,
                'partialDownload': self.partialDownload,
                'downloadRange': self.downloadRange,
                'incremental': self.incremental,
                'sendToStarburst': self.sendToStarburst,
                'extractExif': self.extractExif,
                'extractLabels': self.extractLabels,
                'config': self.config,
                'updateStatisticsAfter': self.updateStatisticsAfter
                }
            thread = threading.Thread(target = worker, name = self.threads[tId]['name'], kwargs = kwargs)
            thread.start()
            if not self.threadsLaunched:
                self.threadsLaunched = True

        # Update progress
        if not self.threadsLaunched:
            update_progress_page(progressPage=self.progressPage, progress="init-threads-failed")
        # pprint.pprint(threads)
    
    # Step 5 ################## Process threads queues #################
    def process_thread_queues(self):

        threadsToCheck = []
        for tId in self.threads.keys():
            threadsToCheck.append(tId)

        # Check threads' status
        while True:
            if threadsToCheck:
                # threadsFinalized is per iterated processing of threadsToCheck list. It is zeroed after each processing.
                threadsFinalized = []
                for tId in threadsToCheck:
                    while True:
                        # queueSize
                        queueSize = self.threads[tId]['threadQueue'].qsize()
                        if queueSize:
                            logger.debug(f"{tId} queue size: {queueSize}")
                            _stats = self.threads[tId]['threadQueue'].get()
                            self.threads[tId]['threadQueue'].task_done()

                            # Following signals the end of processing
                            if _stats['processedFilesCount'] == -1:
                                threadsFinalized.append(tId)
                            else:
                                self.numberOfObjectsProcessed  +=  _stats['processedFilesCount']
                                self.numberOfObjectsSkipped += _stats['skippedFilesCount']
                        # if queueSize is zero then break the while to continue with the next thread's queue 
                        else:
                            break

            for tId in threadsFinalized:
                threadsToCheck.remove(tId)

            if self.numberOfObjectsProcessed:
                logger.info(f"Number of processed objects: {self.numberOfObjectsProcessed}")
            if self.numberOfObjectsSkipped:
                logger.info(f"Number of skipped objects: {self.numberOfObjectsSkipped}")
            
            self.progress = int(100 * (self.numberOfObjectsProcessed + self.numberOfObjectsSkipped) / self.numberOfObjects)
            logger.debug(f"Progress: %{int(100 * (self.numberOfObjectsProcessed + self.numberOfObjectsSkipped) / self.numberOfObjects)}")
            update_progress_page(progressPage=self.progressPage, progress=self.progress)
        
            # Break if all the threads have exited
            if threadsToCheck:
                time.sleep(self.intervalToCheckThreads)
            else:
                # If somehow any of the threads goes rogue or get terminated before completion, report that the processing is done.  
                if int(100 * (self.numberOfObjectsProcessed + self.numberOfObjectsSkipped) / self.numberOfObjects) != 100:
                    logger.debug(f"Progress: %100, however some objects have been missed.")
                    self.progress = 100
                    update_progress_page(progressPage=self.progressPage, progress=self.progress)
                break

        if(self.sendToStarburst):
            self.progress = 101
            update_progress_page(progressPage=self.progressPage, progress=101)

        while True:
            # Main thread is always part of the thread list, so when the count is 1, that means all other child threads have exited already.
            if threading.enumerate():
                _threadsFound = False
                for thread in threading.enumerate():
                    mo = rexThread.search(thread.name)
                    if mo:
                        logger.debug(f"Active thread found: {thread.name}")
                        _threadsFound = True
                
                if _threadsFound:
                    time.sleep(self.intervalToCheckThreads)
                    logger.info("Waiting on it's child threads to terminate...")
                else:
                    logger.info("All threads have completed.")
                    break
            else:
                break

        
        self.progress = 200
        update_progress_page(progressPage=self.progressPage, progress=self.progress)

        logger.info("##########" + " Ended execution " + "###########")

        return True
    
if __name__ == '__main__':
    config = config

    # Step 1 ###################### Main initialization ######################
    main = Main(config = config)
    main.initialize()
    if main.initializationFailed:
        logger.critical(f"Initialization failed!")
        sys.exit()

    # Step 2 ###################### Starburst initialization ######################
    main.check_starburst_connectivity()
    if main.starburst.clusterAvailable:
        main.create_schema_table()
    else:
        logger.error(f"Startburst/DDAE Cluster is not available. Skipping Starburst updates.")

    # Step 2 ###################### Image processing ######################
    main.initialize_threads()
    if main.threadsLaunched:
        main.process_thread_queues()    

    








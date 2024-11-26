import os, sys, pathlib, subprocess, logging, re, pprint
import logging.config
import boto3
from boto3 import Session
from botocore.config import Config as s3Configure

# Add modules to sys.path
sys.path.append(os.path.join(pathlib.Path(__file__).resolve().absolute().parents[2], 'modules'))

from main_config.main_config import Config
from log_config.log_config import LoggingConf

# Initialize config
configObj = Config()

if __name__ == "__main__":
    configObj.update_main_conf()
    configObj.set_defaults()
    configObj.initialize_logging_conf()
    configObj.update_logging_conf()

config = configObj.mainConfigDict

# Set logging
loggingConfig = LoggingConf()
logging.config.dictConfig(loggingConfig.config)
logger = logging.getLogger(__name__)
# print(logger.handlers)
# print(logger)

class S3(object):

    def __init__(self, config = None):

        config = config
        s3Config = config['s3']
        # Set logging
        loggingConfig = LoggingConf()
        logging.config.dictConfig(loggingConfig.config)
        logger = logging.getLogger(__name__)
        # print(logger.handlers)
        # print(logger)
        logger.setLevel(config['logging']['level'])

        self.endpoint = s3Config["endpoint"]
        self.useSSL = True if 'https' in s3Config["endpoint"] else False
        self.key = s3Config["key"]
        self.secret = s3Config["secret"]
        self.bucketName = s3Config["bucketName"]
        self.pageSize = s3Config["pageSize"]
        self.bucketVerified = False
        self.objectsDownloaded = False
        self.dryRun = config['runTime']['dryRun']
        self.verifyCertificate = s3Config['verifyCertificate']


        logger.info("#####" + " Started execution " + "###########")
    
    def create_client(self):

        # Create S3 config object
        _config = s3Configure(signature_version='s3v4', s3={'addressing_style': 'path',},
                            connect_timeout = 3,
                            read_timeout = 3,
                            retries = {
                            'max_attempts': 3,
                            'mode': 'standard'
                            })

        #### Create a session client
        self.client = boto3.client('s3', use_ssl=self.useSSL, verify=self.verifyCertificate, endpoint_url=self.endpoint,
                aws_access_key_id=self.key, aws_secret_access_key=self.secret, 
                aws_session_token=None, 
                config=_config)
        
        # Create transfer configuration
        self.transferConfig=boto3.s3.transfer.TransferConfig(multipart_threshold=8388608, max_concurrency=10, multipart_chunksize=8388608, num_download_attempts=5, max_io_queue=100, io_chunksize=262144, use_threads=True)
    
    def verify_bucket(self):

        try:
            response = self.client.head_bucket(
                Bucket= self.bucketName,
                ExpectedBucketOwner=self.key
            )
            # print(response['ResponseMetadata']['HTTPStatusCode'])
            self.bucketVerified = True
        except Exception as err:
            logger.critical(err)
            self.bucketVerified = False
        
        logger.info("Bucket verified") if self.bucketVerified else logger.critical("Bucket could not be verified!")

    def create_objects(self, numberOfObjects = 100, folderDepth = 3, extension = None):

        baseDir = ""
        for d in range(1,folderDepth+1,1):
            baseDir += "folder" + str(d) + "/"

        for i in range(1,numberOfObjects+1,1):
            if extension:
                objectName = baseDir + "object" + str(i) + "." + extension
            else:
                objectName = baseDir + "object" + str(i)
            try:
                self.client.put_object(Body= f"object{str(i)}".encode("UTF-8"), Metadata = {"name": f"object{str(i)}"}, 
                                    Key = objectName, Bucket = self.bucketName)
                logger.debug(f"{objectName} has been uploaded.")
            except Exception as err:
                logger.error(f"Could not upload {objectName}")

    def get_count_of_objects_and_paginators(self):

        _paginator = self.client.get_paginator("list_objects_v2")
        _iterator = _paginator.paginate(Bucket = self.bucketName,
                PaginationConfig={
                    'PageSize': self.pageSize
                }
            )

        # Get the number of pages and objects
        _numOfPages = 0
        _numOfObjects = 0

        for _page in _iterator:
            _numOfObjects += _page["KeyCount"]
            if _page["KeyCount"] > 0:
                _numOfPages += 1


        self.numOfPages = _numOfPages
        self.numOfObjects = _numOfObjects

        logger.info(f"Number of pages with page size of {self.pageSize}: {self.numOfPages}")
        logger.info(f"Number of objects: {self.numOfObjects}")

    def delete_objects(self):

        _paginator = self.client.get_paginator("list_objects_v2")
        _iterator = _paginator.paginate(Bucket = self.bucketName,
                PaginationConfig={
                    'PageSize': self.pageSize
                }
            )

        # Get the number of pages and objects
        _numOfPages = 0
        _numOfObjects = 0
        _numOfObjectsDeleted = 0

        for _page in _iterator:
            _numOfPages += 1

            _objectList = []

            if _page['KeyCount'] > 0:
                _numOfObjects += _page["KeyCount"]
                for _object in _page["Contents"]:
                    _objectList.append(
                        { 'Key': _object['Key'] }
                    )
                    
                try:
                    response = s3.client.delete_objects(Bucket = self.bucketName,
                                                        Delete = { 'Objects': _objectList,
                                                                'Quiet': False }
                                                        )
                    logger.info(f"{_numOfObjects} objects has been deleted for page: {_numOfPages}.")
                    _numOfObjectsDeleted += _numOfObjects
                except Exception as err:
                    logger.critical(err)

        logger.info(f"Total number of {_numOfObjects} objects has been deleted.")

    def download_objects(self, listOfObjects = None, incremental = False, extensions = None, partialDownload = True, downloadRange = None, tempPath = 'temp'):
        filteredObjects = []
        regexExtensions = None
        regexSearchString = None
        
        if extensions:
            for extension in extensions:
                if regexSearchString:
                    regexSearchString += '|' + extension
                else:
                    regexSearchString = extension 

            regexExtensions = re.compile(rf"\.({regexSearchString})$", re.IGNORECASE)
            
        if extensions:
            for key in listOfObjects:
                mo = regexExtensions.search(key)
                if mo:
                    filteredObjects.append(key)
        else:
            filteredObjects = listOfObjects
        
        processingDict = {}
        fileBaseName = 'file'
        counter = 0

        for key in filteredObjects:
            _processed = False
            _exists = False

            try:
                _response = self.client.head_object(Bucket = self.bucketName, Key = key)
                if _response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    _exists = True
                    if 'x-amz-meta-processed' in _response['ResponseMetadata']['HTTPHeaders']:
                        _processed = True
                        logger.debug(f"Object {key} has been processed before.")
            except Exception as err:
                logger.error(f"Couldn't read object metadata for object: {key}")

            if _exists:
                if not incremental or (incremental and not _processed): 
                    counter += 1
                    try:
                        if partialDownload:
                            logger.debug(f"Downloading first {downloadRange} Bytes of {key}...")
                            response = self.client.get_object(Bucket = self.bucketName, Key = key, Range = f'bytes=0-{str(downloadRange)}')
                        else:
                            logger.debug(f"Downloading the object {key} fully...")
                            response = self.client.get_object(Bucket = self.bucketName, Key = key)
                        body = response['Body']
                        filePath = os.path.join(tempPath, fileBaseName + '_' + str(counter))
                        
                        with open(file = filePath, mode = "wb") as file:
                            file.write(body.read())

                        processingDict[filePath] = {'key': key, 'processed': False}
                        logger.debug(f"{key} has been downloaded successfully as {filePath}")

                    except Exception as err:
                        logger.error(f"Could not download object {key} successfully. Skipping...")
                        logger.error(err)

        return processingDict


    def download_video_object(self, videoObject = None, incremental = False, tempPath = 'temp', videoObjectCounter = 0):

        key = videoObject
        processingDict = {}
        fileBaseName = 'vfile'
        counter = videoObjectCounter
        _exists = False

        try:
            _response = self.client.head_object(Bucket = self.bucketName, Key = key)
            if _response['ResponseMetadata']['HTTPStatusCode'] == 200:
                _exists = True
                if 'x-amz-meta-processed' in _response['ResponseMetadata']['HTTPHeaders']:
                    _processed = True
                    logger.debug(f"Object {key} has been processed before.")
        except Exception as err:
            logger.error(f"Couldn't read object metadata for object: {key}")
        
        if _exists:
            if not incremental or (incremental and not _processed): 
                try:
                    logger.debug(f"Downloading the video object {key}...")
                    response = self.client.get_object(Bucket = self.bucketName, Key = key)
                    body = response['Body']
                    filePath = os.path.join(tempPath, fileBaseName + '_' + str(counter))
                    
                    with open(file = filePath, mode = "wb") as file:
                        file.write(body.read())

                    processingDict[filePath] = {'key': key, 'processed': False}
                    logger.debug(f"{key} has been downloaded successfully as {filePath}")

                except Exception as err:
                    logger.error(f"Could not download object {key} successfully. Skipping...")
                    logger.error(err)
        
        return processingDict


    def assign_pages_to_threads(self, threads = None):

        if not threads:
            logger.critical("No threads to assign pages to!")
            return False
        
        _paginator = self.client.get_paginator("list_objects_v2")
        _iterator = _paginator.paginate(Bucket = self.bucketName,
                PaginationConfig={
                    'PageSize': self.pageSize
                }
            )

        # Get the number of pages and objects
        _numOfPages = 0
        _numOfObjects = 0
        _pages = []
        for _page in _iterator:
            _numOfObjects += _page["KeyCount"]
            if _page["KeyCount"] > 0:
                _numOfPages += 1
                _pages.append(_page)
        
        _pageNumber = 0
        for key in threads.keys():
            for i in range(1,threads[key]['numberOfPages']+1,1):
                threads[key]['pages'].append(_pages[_pageNumber])
                _pageNumber += 1
    
    def update_metadata(self, processingDict = None, threadQueue = None, updateStatisticsAfter = 10):

        if not (processingDict or threadQueue):
            logger.debug(f"No objects to update the metadata for. Quiting...")
            sys.exit()
        
        skippedFilesCount = 0
        processedFilesCount = 0
        counter = 0
        metadata = []

        for filePath in processingDict.keys():
            _metadata = {}
            if 'exif' in processingDict[filePath].keys():
                for k,v in processingDict[filePath]['exif'].items():
                    # Stringify and change the character representation to confirm ASCII.
                    # S3 only supports ASCII encoding with REST calls
                    v = str(v).replace("ı", "i")
                    v = v.encode("ASCII","ignore").decode("ASCII")
                    _metadata.update({k.lower():v})
     
            if 'labels' in processingDict[filePath].keys():
                _labelNumber = 0
                for l in processingDict[filePath]['labels']:
                    _labelNumber += 1
                    _metadata.update({f"label{_labelNumber}":l})
            
            
            _key = processingDict[filePath]['key']
            _metadata.update({'processed': 'True'})
            
            try:
                _response = self.client.head_object(Bucket = self.bucketName, Key = _key)
                _eTag = _response['ETag']
                # print(f"ETag: {_eTag}")
                # This copies object in place verifying the etag
                if not self.dryRun:
                    _response = self.client.copy_object(CopySource = {'Bucket': self.bucketName, 'Key': _key}, 
                                        Bucket = self.bucketName, Key = _key, 
                                        CopySourceIfMatch = _eTag,
                                        Metadata = _metadata,
                                        MetadataDirective = 'REPLACE'
                                    )
                else:
                    logger.debug(f"Dry run is set, skipping object '{_key}'...")

                processedFilesCount += 1
                
                logger.debug(f"Updated metadata for {_key}")
                # Update the metadata list, which will be returned to the calling module
                _metadata.update({'key': _key})
                metadata.append(_metadata)
                
            except Exception as err:
                skippedFilesCount += 1
                logger.error(f"Could not update metadata for {_key} properly. Skipping object...")
                logger.error(err)
            
            counter += 1

            if counter == processedFilesCount:
                # Update thread status
                threadQueue.put({'processedFilesCount': processedFilesCount, 'skippedFilesCount': skippedFilesCount})
                # logger.debug(f"Thread queue size is {threadQueue.qsize()}")
                counter = 0
                processedFilesCount = 0
                skippedFilesCount = 0

        # If there are number of processed files, and the count of them is less then "counter", then there is still stuff to be put in the queue.
        if counter:
            threadQueue.put({'processedFilesCount': processedFilesCount, 'skippedFilesCount': skippedFilesCount})
            counter = 0
            processedFilesCount = 0
            skippedFilesCount = 0

        # Processing of the object is over. -1 for the stats signals that.
        threadQueue.put({'processedFilesCount': -1, 'skippedFilesCount': -1})
        return metadata

    def __exit__(self):
        logger.info("##########" + " Ended execution " + "###########")

    def upload_a_single_object(self):
        _metadata = {"hello": "world", "bye": "hell"}
        s3.client.put_object(
                            Bucket = self.bucketName, 
                            Key = "object1", 
                            Metadata = _metadata
                            )

if __name__ == '__main__':

    s3 = S3(config = config)
    s3.create_client()
    s3.verify_bucket()
    # s3.upload_a_single_object()
    # s3.create_objects(numberOfObjects = 58, extension = "bmp", folderDepth=2)
    # s3.get_count_of_objects_and_paginators()
    # s3.delete_objects()
    # s3.download_objects()
    s3.__exit__()
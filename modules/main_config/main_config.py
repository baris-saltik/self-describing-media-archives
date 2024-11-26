import os, pathlib, sys, pprint
from yaml import load, dump, Loader, Dumper

modulesPath = sys.path.append(os.path.join(pathlib.Path(__file__).resolve().parents[2], "modules"))

from log_config.log_config import LoggingConf 

configBasePath = os.path.join(pathlib.Path(__file__).resolve().absolute().parents[2], 'config')
configPath = os.path.join(pathlib.Path(__file__).resolve().absolute().parents[2], 'config', 'main.yaml')
loggingConfigPath = os.path.join(pathlib.Path(__file__).resolve().absolute().parents[2], 'config', 'logging.yaml')

class Config(object):

    def __init__(self, configPath = configPath):

        self.configPath = configPath
        self.mainConfigDict = {}

        if not os.path.exists(configBasePath):
            os.makedirs(configBasePath)
        
        if not os.path.isfile(configPath):
            self.set_defaults()
            self.update_main_conf()     
        else:
            with open(configPath, "r") as configFile:
                self.mainConfigDict = load(stream = configFile, Loader = Loader)
            self.set_defaults()

    def set_defaults(self):

        ### Defaults
        # logging
        logging = {
            'level': 'DEBUG',
            'handlerLevel': 'DEBUG',
            'maxBytes': 1000,
            'backupCount': 2
        }

        if self.mainConfigDict.get('logging'):
            for k, v in logging.items():
                self.mainConfigDict['logging'].setdefault(k,v)
        else:
            self.mainConfigDict['logging'] = logging

        # s3
        s3Defaults = {
            # ECS Test Drive credentials
            'endpoint': 'https://object.ecstestdrive.com',
            'key': '',
            'secret':'',
            'bucketName': 'mediaBucket',
            'pageSize': 10,
            "verifyCertificate": True
        }

        if self.mainConfigDict.get('s3'):
            for k, v in s3Defaults.items():
                self.mainConfigDict['s3'].setdefault(k,v)
        else:
            self.mainConfigDict['s3'] = s3Defaults

        # runTime
        runTimeDefaults = {
            'maxNumberOfThreads': 8,
            'tempPath': 'temp',
            'partialDownload': True,
            'downloadRange': 102400,
            'incremental': False,
            'extractExif': True,
            'extractLabels': False,
            'updateStatisticsAfter': 10, # This is in number of objects
            'sendToStarburst': False,
            'intervalToCheckThreads': 3, # This is in number of seconds
            'dryRun': False,
            'progressPage': os.path.join(pathlib.Path(__file__).resolve().parents[2], "site", "templates", "progress.html")
        }

        if self.mainConfigDict.get('runTime'):
            for k, v in runTimeDefaults.items():
                self.mainConfigDict['runTime'].setdefault(k,v)
        else:
            self.mainConfigDict['runTime'] = runTimeDefaults

        # Exif
        exifDefaults = {
            'extensions': ['jpg', 'jpeg', 'bmp', 'tiff', 'gif', 'png'],
            'videoExtensions': ['mp4']
        }

        if self.mainConfigDict.get('exif'):
            for k, v in exifDefaults.items():
                self.mainConfigDict['exif'].setdefault(k,v)
        else:
            self.mainConfigDict['exif'] = exifDefaults

        # Vision AI
        visionAIDefaults = {
            'creds': {},
            'credsFilePath': '',
            'testFilePath': "test_image.jpg",
            'testVideoFilePath': 'test_video.mp4'
        }

        if self.mainConfigDict.get('visionAI'):
            for k, v in visionAIDefaults.items():
                self.mainConfigDict['visionAI'].setdefault(k,v)
        else:
            self.mainConfigDict['visionAI'] = visionAIDefaults

        # Starburst/DDAE
        starburstDefaults = {
            "host": "domain-name.galaxy.starburst.io",
            "port": 443,
            "httpScheme": "https",
            "verifyCertificate": False,
            "user": "username/accountadmin",
            "password": "",
            "roleName": "accountadmin",
            "catalog": "iceberg",
            "schema": "media_metadata",
            "table": "media_bucket",
            "bucket": "sdma",
            "schemaLocation": "/media_metadata",
            "columns": ["key", "createdate", "filesize", "flash", "imagesize", "mimetype", "make", "model", "orientation", "software", "gpsaltitude", "gpslatitude", "gpslongitude", "geolocationbearing", "geolocationcity", "geolocationcountry", "geolocationcountrycode", "geolocationdistance", "geolocationpopulation", "geolocationposition", "geolocationregion", "geolocationtimezone", 
                        "label1","label2","label3","label4","label5","label6","label7","label8","label9","label10","processed"],
            "galaxy": {"accountDomain": "domain-name.galaxy.starburst.io", 
                       "apiKey": "",
                       "apiSecret": ""},
            "type": "Galaxy"

        }
        

        if self.mainConfigDict.get('starburst'):
            for k, v in starburstDefaults.items():
                self.mainConfigDict['starburst'].setdefault(k,v)
        else:
            self.mainConfigDict['starburst'] = starburstDefaults

    def update_main_conf(self, newConfig = None):
        if newConfig:
            self.mainConfigDict = newConfig
        print("Updating main.yaml...")
        with open(self.configPath, "w") as configFile:
            dump(stream = configFile, data = self.mainConfigDict, Dumper=Dumper)

    def initialize_logging_conf(self):
        print("Initializing logging.yaml...")
        loggingConfigObj = LoggingConf()
        loggingConfigObj.create_log_directories()
        loggingConfigObj.create_logging_conf( dynamicPartDict = self.mainConfigDict['logging'])
    
    def update_logging_conf(self, loggingConfigPath = loggingConfigPath):
        print("Updating logging.yaml...")
        # Read existing logging config file
        with open(loggingConfigPath, "r") as loggingConfigFile:
            loggingConfig = load(loggingConfigFile, Loader = Loader)
        
        # Update logging config
        for id in loggingConfig['handlers'].keys():
            if id != 'stream':
                loggingConfig['handlers'][id]["maxBytes"] = self.mainConfigDict['logging']['maxBytes']
                loggingConfig['handlers'][id]["backupCount"]  = self.mainConfigDict['logging']['backupCount']

        for id in loggingConfig['loggers'].keys():
            loggingConfig['loggers'][id]['level'] = self.mainConfigDict['logging']['level']
        
        for id in loggingConfig['handlers'].keys():
            loggingConfig['handlers'][id]['level'] = self.mainConfigDict['logging']['handlerLevel']
        

        # loggingConfig['root']['level'] = self.mainConfigDict['logging']['level']

        # Write the updated config to the disk
        with open(loggingConfigPath, "w") as loggingConfigFile:
            loggingConfigFile.write('# DO NOT edit this file directly. Configure parameters under "logging" key in "main.yaml" file instead\n')
            loggingConfigFile.write(dump(loggingConfig, Dumper = Dumper, default_flow_style = False))


if __name__ == '__main__':
    configObj = Config()
    configObj.set_defaults()
    configObj.initialize_logging_conf()
    configObj.update_logging_conf()
    configObj.update_main_conf()


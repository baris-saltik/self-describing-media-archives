import os, sys, pathlib, subprocess, logging, json, pprint
import logging.config
from google.cloud import vision
from google.cloud import vision_v1

# Add modules to sys.path
sys.path.append(os.path.join(pathlib.Path(__file__).resolve().absolute().parents[2], 'modules'))

# Import additional custom modules
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

class VisionAI(object):

    def __init__(self, config = None ):

        self.tested = False
        config = config
        visionAIConfig = config['visionAI']
        # Set logging
        loggingConfig = LoggingConf()
        logging.config.dictConfig(loggingConfig.config)
        logger = logging.getLogger(__name__)
        logger.setLevel(config['logging']['level'])

        if not visionAIConfig:
            logger.critical("No Vision AI credentials are defined. Quiting...")
            return None
        
        logger.info("#####" + " Started execution " + "###########")

        self.credsFullFilePath = os.path.join(pathlib.Path(__file__).resolve().parents[2], "google_vision_ai", "visionAI_creds.json")
        try:
            with open(file = self.credsFullFilePath, mode="w") as file:
                file.write(json.dumps(visionAIConfig['creds'], sort_keys=True, indent=1))
            logger.debug(f"Created Vision AI config file {self.credsFullFilePath}")
        except Exception as err:
            logger.error(f"Could not create Vision AI config file {self.credsFullFilePath}. Quiting...")
            return None

        self.testFilePath = os.path.join(pathlib.Path(__file__).resolve().parents[2], "google_vision_ai", visionAIConfig["testFilePath"])
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credsFullFilePath
    
    def test_labeling(self):

        if not self.testFilePath:
            logger.critical("No file path designated. Quiting...")
            self.tested = False
            return False

        client = vision.ImageAnnotatorClient()

        try:
            with open(file = self.testFilePath, mode = "rb") as imageFile:
                content = imageFile.read()
            image = vision.Image(content = content)
            response = client.label_detection(image = image)
            

            if response.error.message:
                logger.error(
                    "{}\nFor more info on error messages, check: "
                    "https://cloud.google.com/apis/design/errors".format(response.error.message)
                )
                self.tested = False
            else:
                self.tested = True
                       
        except Exception as err:
            logger.error(f"File {self.testFilePath} could not be opened for test labeling.")
            self.tested = False

    def get_labels(self, fileList = None):

        if not fileList:
            logger.critical("No file path designated. Quiting...")
            return False
    
        client = vision.ImageAnnotatorClient()
        labeledFiles = {}

        logger.info(f"Started labeling files...")
        for filePath in fileList:
            try:
                with open(file = filePath, mode = "rb") as imageFile:
                    content = imageFile.read()
                image = vision.Image(content = content)
                response = client.label_detection(image = image)

                if response.error.message:
                    logger.error(
                        "{}\nFor more info on error messages, check: "
                        "https://cloud.google.com/apis/design/errors".format(response.error.message)
                    )

                labels = response.label_annotations
                
                # logger.debug("#" * 40)
                # logger.debug(f"Labels for {filePath}:")
                
                labeledFiles[filePath] = []
                for label in labels:
                    labeledFiles[filePath].append(label.description.lower())
                logger.debug(f"Labeled {filePath}.")
                # logger.debug("#" * 80)
                
            except Exception as err:
                logger.error(f"File {filePath} could not be opened for labeling. Skipping...")

        # labeledFiles is dictionary, and it has the following structure:
        # labeledFiles = {filePath: labelDescriptions}
        # filePath is a full path, i.e. C:\temp\thread\file1
        # labelDescriptions is a list, i.e. ["Snow", "Winter Sports", "World"]
        return labeledFiles

    def __exit__(self):
        logger.info("##########" + " Ended execution " + "###########")


if __name__ == '__main__':

    visionAi = VisionAI(config = config)
    visionAi.test_labeling()
    if visionAi.tested:
        print("Test successful")
    else:
        print("Test failed")


    # labeledFiles = visionAi.get_labels(fileList = fileList)
    # for k,v in labeledFiles.items():
    #    print(k)
    #    print(v)

    visionAi.__exit__()
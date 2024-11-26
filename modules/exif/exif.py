import os, sys, pathlib, subprocess, logging, json, pprint
import logging.config

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

# Defaults
# path = r"C:\Users\saltib\OneDrive - Dell Technologies\Documents\dokumanlar\resimler\sb\test_data\*"

path = os.path.join(pathlib.Path(__file__).resolve().parents[2], "google_vision_ai" )
videoPath = os.path.join(pathlib.Path(__file__).resolve().parents[2], "google_vision_ai", "test_video.mp4" )
exiftoolFullPath = os.path.join(pathlib.Path(__file__).resolve().parents[2], "exiftool", "exiftool.exe" )

class Exif(object):

    def __init__(self, config = None ):

        config = config
        exifConfig = config['exif']
        logger.setLevel(config['logging']['level'])

        self.extensions = exifConfig["extensions"]

        logger.info("#####" + " Started execution " + "###########")
    
    def extract(self, path = None):

        if not path:
            logger.critical("No path designated. Quiting...")
            return False

        exifProcess = subprocess.Popen(args = [exiftoolFullPath, "-api", "geolocation", "-charset", "UTF8", "-MyTags", "-json", path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
        (output, err) = exifProcess.communicate()

        if not output:
            logger.critical(err.decode('UTF-8'))
            return False
        
        # Load exif data string into a list
        exifData = json.loads(output.decode('UTF-8'))
        logger.info(f'Exif data has been extracted for "{path}"')
        return exifData
    
    def extract_video(self, videoPath = None):

        if not videoPath:
            logger.critical("No path designated. Quiting...")
            return False

        exifProcess = subprocess.Popen(args = [exiftoolFullPath, "-api", "geolocation", "-charset", "UTF8", "-MyTags", "-json", videoPath], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
        # exifProcess = subprocess.Popen(args = [exiftoolFullPath, "-api", "geolocation", "-charset", "UTF8", "-json", videoPath], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)

        (output, err) = exifProcess.communicate()

        # Load exif data string into a list
        exifData = json.loads(output.decode('UTF-8'))
        logger.info(f'Exif data has been extracted for "{videoPath}"')
        return exifData


    def __exit__(self):
        logger.info("##########" + " Ended execution " + "###########")


if __name__ == '__main__':

    exif = Exif(config = config)
    exifData = exif.extract(path = path)
    # exifData = exif.extract_video(videoPath = videoPath)

    pprint.pprint(exifData)

    exif.__exit__()
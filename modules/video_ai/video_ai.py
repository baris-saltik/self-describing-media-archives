import os, sys, pathlib, subprocess, logging, json, pprint, io
import logging.config
from google.cloud import videointelligence

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

class VideoAI(object):

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

        self.testFilePath = os.path.join(pathlib.Path(__file__).resolve().parents[2], "google_vision_ai", visionAIConfig["testVideoFilePath"])
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credsFullFilePath
    
    def test_labeling(self):

        if not self.testFilePath:
            logger.critical("No file path designated. Quiting...")
            self.tested = False
            return False

        video_client = videointelligence.VideoIntelligenceServiceClient()
        features = [videointelligence.Feature.LABEL_DETECTION]

        try:
            with io.open(self.testFilePath , "rb") as movie:
                input_content = movie.read()

            operation = video_client.annotate_video(
                request={"features": features, "input_content": input_content}
            )

            logger.info("\nProcessing video for label annotations:")

            result = operation.result(timeout=90)

            logger.info("\nFinished processing.")
            
            self.tested = True

        except Exception as err:
            logger.error(f"File {self.testFilePath} could not be opened for test labeling.")
            self.tested = False
        
    def get_labels(self, videoPath = None):

        if not videoPath:
            logger.critical("No file path designated. Quiting...")
            return False
        
        video_client = videointelligence.VideoIntelligenceServiceClient()
        features = [videointelligence.Feature.LABEL_DETECTION]

        _topTenLabelsByConfidence = []
        _topTenLabelsByConfidenceWithoutConfidence = []

        try:      
            logger.info("Reading the video file payload...")
            with io.open(videoPath , "rb") as movie:
                input_content = movie.read()

            logger.info("Sending video file payload to Google Cloud Video Intelligence...")
            operation = video_client.annotate_video(
                request={"features": features, "input_content": input_content}
            )

            logger.info("Processing video for label annotations:")

            result = operation.result(timeout=90)

            # Process shot level label annotations
            # 0 element is the segment_label_annotations
            _labels = []
            _labelCount = 0
            
            _shot_labels = result.annotation_results[0].shot_label_annotations
            for i, shot_label in enumerate(_shot_labels):
                _labelCount += 1
                # logger.debug("Shot label description: {}".format(shot_label.entity.description))
                
                # for category_entity in shot_label.category_entities:
                #   logger.debug("\tLabel category description: {}".format(category_entity.description))

                _confidence = 0
                for i, shot in enumerate(shot_label.segments):
                    start_time = (
                        shot.segment.start_time_offset.seconds
                        + shot.segment.start_time_offset.microseconds / 1e6
                    )
                    end_time = (
                        shot.segment.end_time_offset.seconds
                        + shot.segment.end_time_offset.microseconds / 1e6
                    )
                    _positions = "{}s to {}s".format(start_time, end_time)
                    _current_confidence = shot.confidence
                    # logger.debug("\tSegment {}: {}".format(i, _positions))
                    # logger.debug("\tConfidence: {}".format(_current_confidence))

                    if _current_confidence > _confidence:
                        _confidence = _current_confidence

                _labels.append((shot_label.entity.description, _confidence))
   


            _confidenceList = [ float(i[1]) for i in _labels ]
            _sortedConfidenceList = sorted(_confidenceList, key=float, reverse=True)

            _top10sortedConfidenceList = _sortedConfidenceList[0:10]

            # print(_top10sortedConfidenceList)

            _counter = 0
            for label in _labels:
                if float(label[1]) in _top10sortedConfidenceList:
                    _topTenLabelsByConfidence.append((label))
                    _topTenLabelsByConfidenceWithoutConfidence.append(label[0])
                    _counter += 1
                if _counter >= 10:
                    break


            # logger.debug(f"There are {_labelCount} labels in this video.")
            # logger.debug("Top 10 Labels By Confidence: ")

            # logger.debug([f"Label: {label[0]} - Confidence: {label[1]}" for label in _topTenLabelsByConfidence])

            logger.info(f"Finished labeling video file: {videoPath}.")
            

        except Exception as err:
            logger.error(f"File {videoPath} could not be opened for labeling. Skipping...")
            logger.error(err)

        # This is not used but defined for future uses.
        labelsWithConfidence = _topTenLabelsByConfidence
        labels = _topTenLabelsByConfidenceWithoutConfidence

        return labels
        

    def __exit__(self):
        logger.info("##########" + " Ended execution " + "###########")


if __name__ == '__main__':

    videoAI = VideoAI(config = config)

    '''
    videoAI.test_labeling()

    if videoAI.tested:
        print("Test successful")
    else:
        print("Test failed")
        videoAI.__exit__()
    '''

    videoAI.get_labels(videoAI.testFilePath)

    videoAI.__exit__()
import logging.config, sys, os, re, pprint, json, threading, queue
import os, pathlib, flask, datetime, logging, time
from flask import Flask, render_template, redirect, url_for, session, request, send_from_directory
from flask_wtf import FlaskForm


# Step 1 ################## Read main config and update logging config #################
# Set the logging.conf file first before other modules loads it.
from modules.main_config.main_config import Config

# Create default config which is going to be used for the rest of the custom modules.
configObj = Config()
configObj.update_main_conf()
configObj.set_defaults()
configObj.initialize_logging_conf()
configObj.update_logging_conf()
config = configObj.mainConfigDict

from modules.log_config.log_config import LoggingConf
from sdma_main import Main
from modules.vision_ai.vision_ai import VisionAI
from modules.starburst_data.starburst_data import StarburstData
from modules.galaxy_api.galaxy_api import GalaxyAPI
from modules.s3.s3 import S3


# pprint.pprint(config)

# Initialize logging
loggingConfig = LoggingConf().config
logging.config.dictConfig(loggingConfig)
logger = logging.getLogger('sdma_web')


# Step 2 ################## Load the rest of the custom modules #################
# Load the custom modules which relies on logging configuration

from modules.form.form import create_form

### Regular expressions

rexS3Keys = re.compile(r"s3(\w)(.*)Field")
rexRunTimeKeys = re.compile(r"runTime(\w)(.*)Field")
rexLoggingKeys = re.compile(r"logging(\w)(.*)Field")
rexGalaxyKeys = re.compile(r"starburstGalaxy(\w)(.*)Field")
rexStarburstKeys = re.compile(r"starburst(\w)(.*)Field")
rexVisionAIKeys = re.compile(r"visionAI(\w)(.*)Field")

# Step 3 ################## Initialize Flask app #################
progressPage = config['runTime']['progressPage']
static_folder = os.path.join(pathlib.Path(__file__).resolve().absolute().parents[0], 'site', 'static')
template_folder = os.path.join(pathlib.Path(__file__).resolve().absolute().parents[0], 'site', 'templates')

app = Flask(import_name = __name__, static_folder = static_folder, template_folder = template_folder)
app.config['config'] = config


### Read the progress
def read_progress(progressPage = progressPage):
    # Progress codes:
    #   -1: Not started
    #    0-100: Actual progress 
    #    101: Starburst/DDAE activity
    #    200: Successful finish 
        
    if progressPage:
        try:
            with open(file = progressPage, mode="r") as file:
                progress = file.read().rstrip("\n")
        except Exception as err:
            logger.error(f"Could not update progress page: {progressPage}")
            logger.error(err)
            progress = 200

    else:
        logger.error("No progress page to update!")
        progress = 200

    return progress

### Update config
def update_config(config = config, mainForm = None):

    logger.info("Updating config values...")
    # pprint.pprint(type(mainForm.data))

    if not mainForm.data:
        logger.info("There is nothing update the main config")
        return False

    
    for key in mainForm.data.keys():
        mo = rexS3Keys.search(key)
        if mo:
            # Set the respective value in the main conf
            try:
                if 'endpoint' in key.lower():
                    confKey = 'endpoint'
                else:
                    confKey = mo.group(1).lower() + mo.group(2)
                    #print("s3: " + mo.group(1).lower() + mo.group(2) + " = " + str(mainForm.data[key]))
                config['s3'][confKey] = mainForm.data[key]
                continue
            except Exception as err:
                logger.debug(err)

        mo = rexRunTimeKeys.search(key)
        if mo:
            # Set the respective value in the main conf
            try:
                confKey = mo.group(1).lower() + mo.group(2)
                #print("runTime: " + mo.group(1).lower() + mo.group(2) + " = " + str(mainForm.data[key]))
                config['runTime'][confKey] = mainForm.data[key]
                continue
            except Exception as err:
                logger.debug(err)

        mo = rexLoggingKeys.search(key)
        if mo:
            # Set the respective value in the main conf
            try:
                confKey = mo.group(1).lower() + mo.group(2)
                #print("logging: " + mo.group(1).lower() + mo.group(2) + " = " + str(mainForm.data[key]))
                config['logging'][confKey] = mainForm.data[key]
                continue
            except Exception as err:
                logger.error(err)

        mo = rexGalaxyKeys.search(key)
        if mo:
            # Set the respective value in the main conf
            try:
                confKey = mo.group(1).lower() + mo.group(2)
                #print("starburst: galaxy: " + mo.group(1).lower() + mo.group(2) + " = " + str(mainForm.data[key]))
                config['starburst']['galaxy'][confKey] = mainForm.data[key]
                continue
            except Exception as err:
                logger.debug(err)

        mo = rexStarburstKeys.search(key)
        if mo:
            # Set the respective value in the main conf
            try:
                confKey = mo.group(1).lower() + mo.group(2)
                #print("starburst: " + mo.group(1).lower() + mo.group(2) + " = " + str(mainForm.data[key]))
                config['starburst'][confKey] = mainForm.data[key]
                continue
            except Exception as err:         
                logger.debug(err)

        mo = rexVisionAIKeys.search(key)
        if mo:
            # Set the respective value in the main conf
            try:
                confKey = mo.group(1).lower() + mo.group(2)
                #print("visionAI: " + mo.group(1).lower() + mo.group(2) + " = " + str(mainForm.data[key]))
                if confKey == 'creds':
                    config['visionAI'][confKey] = eval(mainForm.data[key].replace("\n", ""))
                else:
                    config['visionAI'][confKey] = mainForm.data[key]
                continue
            except Exception as err:
                logger.debug(err)


    # print(type(mainForm.data))
    # print("##################################### Test #########################################################")
    # pprint.pprint(config)

    return True

# logger.info(__file__)

app.config['SECRET_KEY'] = "myVerySecretKey"
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

# Cache controls
@app.before_request
def before_request():
    flask.session.permanent = True
    app.permanent_session_lifetime = datetime.timedelta(minutes=20)
    flask.session.modified = True

@app.after_request
def add_header(response):
    """
    Add headers to both force latest IE rendering engine or Chrome Frame,
    and to cache the rendered page for 10 minutes.
    """
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"

    return response


@app.route('/progress', methods = ['GET', 'POST'])
def progress():
    progress = read_progress()
    return progress

    
@app.route('/', methods = ['GET', 'POST'])
def index():

    ############## Initialize the form ################
    config = Config().mainConfigDict
    # print(config)
    mainForm = create_form(config = config)
    # print(mainForm.data)
    defaultVars = dict(s3Secret = config['s3']['secret'], 
                       starburstPassword = config['starburst']['password'],
                       starburstGalaxyApiSecret = config['starburst']['galaxy']['apiSecret'],
                       message = []
                       )

    if request.method == 'GET':
        
        # This reads the configuration from the disk again
        config = Config().mainConfigDict
        mainForm = create_form(config = config)
        logger.setLevel(config['logging']['level'])

        try:
            # Set values for the boolean form fields
            session["operationGet"]  = True
            session["testPassed"] = False

            # Check if there is a sdma_main running in the back-end
            _progress = read_progress()
            session['runInitiated'] = True if int(_progress) != 200 else False
                
            session['extractExif'] = config['runTime']['extractExif']
            session['extractLabels'] = config['runTime']['extractLabels']
            session['incremental'] = config['runTime']['incremental']
            session['partialDownload'] = config['runTime']['partialDownload']
            session['sendToStarburst'] = config['runTime']['sendToStarburst']
            session['dryRun'] = config['runTime']['dryRun']
            session['starburstVerifyCertificate'] = config['starburst']['verifyCertificate']
            session['s3VerifyCertificate'] = config['s3']['verifyCertificate']
            
        except Exception as err:
            return redirect(url_for('page_not_found'))
        
        return render_template('index.html', mainForm = mainForm, defaultVars = defaultVars)
    
    if request.method == 'POST':

        session["operationGet"] = False
        
        # If Reset button is pressed, reset everything
        if('resetButton' in request.form):
            # Reset data
            config = Config().mainConfigDict
            logger.setLevel(config['logging']['level'])
            del(request.form)
            del(mainForm)
            del(defaultVars)
            mainForm = create_form(config = config)
            defaultVars = dict(s3Secret = config['s3']['secret'], 
                    starburstPassword = config['starburst']['password'],
                    starburstGalaxyApiSecret = config['starburst']['galaxy']['apiSecret'],
                    message = []
                    )
            
            # Set values for the boolean form fields
            session["operationGet"]  = True
            session["testPassed"] = False
            session['runInitiated'] = False
            session['extractExif'] = config['runTime']['extractExif']
            session['extractLabels'] = config['runTime']['extractLabels']
            session['incremental'] = config['runTime']['incremental']
            session['partialDownload'] = config['runTime']['partialDownload']
            session['sendToStarburst'] = config['runTime']['sendToStarburst']
            session['dryRun'] = config['runTime']['dryRun']
            session['starburstVerifyCertificate'] = config['starburst']['verifyCertificate']
            session['s3VerifyCertificate'] = config['s3']['verifyCertificate']

            logger.debug("Page has been resetted.")
            return render_template('index.html', mainForm = mainForm, defaultVars = defaultVars)

        if('testButton' in request.form):

            print("############################### TEST ################################")
            logger.info("Test initiated.")
            session["testPassed"] = False
            defaultVars['message'] = []

            # Update active config with the form data
            if not update_config(config = config, mainForm = mainForm):
                defaultVars['message'].append("Failed to update the configuration")
                return render_template('index.html', mainForm = mainForm, defaultVars = defaultVars)

            logger.setLevel(config['logging']['level'])

            # Check s3 connectivity
            try:
                _s3 = S3(config = config)
                _s3.create_client()
                _s3.verify_bucket()

                if _s3.bucketVerified:
                    session["testPassed"] = True
                    logger.info("Connection to S3 bucket was successful")
                else:
                    defaultVars['message'].append("Failed connecting to S3 bucket")
                    logger.error("Failed connecting to S3 bucket")

                _s3.__exit__()
            except Exception as err:
                session["testPassed"] = False
                defaultVars['message'].append("Failed connecting to S3 bucket")
                logger.error("Failed connecting to S3 bucket")

            # Check connectivity to Starburst
            if config['runTime']['sendToStarburst']:
                try:
                    _starburst = StarburstData(config = config)
                    _starburst.create_session()
                    
                    if _starburst.clusterAvailable:
                        logger.info("Connection to Starburst/DDAE was successful")
                    else:
                        session["testPassed"] = False
                        defaultVars['message'].append("Failed connecting to Starburst/DDAE")
                        logger.error(f"Failed connecting to Starburst/DDAE")
                    
                    _starburst.__exit__()
                except Exception as err:
                    session["testPassed"] = False
                    defaultVars['message'].append("Failed connecting to Starburst/DDAE")
                    logger.error(f"Failed connecting to Starburst/DDAE")



            # Check vision AI connectivity
            if config['runTime']['extractLabels']:
                # Vision AI connectivity
                try:
                    _visionAI = VisionAI(config = config)
                    _visionAI.test_labeling()

                    if _visionAI.tested:
                        logger.info("Labeling the test image was successful")
                    else:
                        session["testPassed"] = False
                        defaultVars['message'].append("Failed labeling a test image")
                        logger.error(f"Failed labeling a test image")

                except Exception as err:
                    session["testPassed"] = False
                    defaultVars['message'].append("Failed labeling a test image")
                    logger.error("Failed labeling a test image")

                
            # Update session variables
            session['runInitiated'] = False
            session['extractExif'] = config['runTime']['extractExif']
            session['extractLabels'] = config['runTime']['extractLabels']
            session['incremental'] = config['runTime']['incremental']
            session['partialDownload'] = config['runTime']['partialDownload']
            session['sendToStarburst'] = config['runTime']['sendToStarburst']
            session['dryRun'] = config['runTime']['dryRun']
            session['starburstVerifyCertificate'] = config['starburst']['verifyCertificate']
            session['s3VerifyCertificate'] = config['s3']['verifyCertificate']

            defaultVars['s3Secret'] = mainForm.data['s3SecretField']
            defaultVars['starburstPassword'] = mainForm.data['starburstPasswordField']
            defaultVars['starburstGalaxyApiSecret'] = mainForm.data['starburstGalaxyApiSecretField']


            return render_template('index.html', mainForm = mainForm, defaultVars = defaultVars)
        
        if('saveButton' in request.form):

            logger.info("Save config initiated.")
            defaultVars['message'] = []

            session["operationGet"]  = True
            session['runInitiated'] = False

            # Update active config with the form data
            if not update_config(config = config, mainForm = mainForm):
                defaultVars['message'].append("Failed to update the configuration")
                return render_template('index.html', mainForm = mainForm, defaultVars = defaultVars)

            # Following can run directly without sending the "config" dictionary for "config" dictionary is a pointer to Config.
            configObj = Config()
            configObj.update_main_conf(newConfig = config)
            logger.setLevel(config['logging']['level'])

            defaultVars['s3Secret'] = mainForm.data['s3SecretField']
            defaultVars['starburstPassword'] = mainForm.data['starburstPasswordField']
            defaultVars['starburstGalaxyApiSecret'] = mainForm.data['starburstGalaxyApiSecretField']

            return render_template('index.html', mainForm = mainForm, defaultVars = defaultVars)
        
        if('runButton' in request.form):
            
            logger.info("Run initiated.")
            defaultVars['message'] = []

            def main_thread(config = config):

                # Step 1 ###################### Main initialization ######################
                main = Main(config = config)
                main.initialize()
                if main.initializationFailed:
                    logger.critical(f"Initialization failed!")
                    return False

                # Step 2 ###################### Starburst initialization ######################
                if config['runTime']['sendToStarburst']:
                    main.check_starburst_connectivity()
                    if main.starburst.clusterAvailable:
                        main.create_schema_table()
                    else:
                        logger.error(f"Startburst/DDAE Cluster is not available. Skipping Starburst updates.")
                        return False


                # Step 2 ###################### Image processing ######################
                main.initialize_threads()
                if main.threadsLaunched:
                    main.process_thread_queues()
                else:
                    logger.error(f"Could not launch workers to do the tasks. See the logs for details.")  
                    return False


            print("############################### RUN ################################")
            
            # Update active config with the form data
            if not update_config(config = config, mainForm = mainForm):
                defaultVars['message'].append("Failed to update the configuration")
                return render_template('index.html', mainForm = mainForm, defaultVars = defaultVars)
            
            logger.setLevel(config['logging']['level'])

            logger.info("Launching sdma_main...")
            mainThread = threading.Thread(target = main_thread, name = "sdmaMainLanucher", kwargs={'config': config})
            mainThread.start()

            session['runInitiated'] = True

            return render_template('index.html', mainForm = mainForm, defaultVars = defaultVars)

@app.errorhandler(404)
def page_not_found(error):
    return render_template('page_not_found.html'), 404


if __name__ == '__main__':
    app.run( host = '0.0.0.0', port = 5000, debug = False, ssl_context = 'adhoc' )
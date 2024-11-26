import logging.config
import os, pathlib, sys, logging
from flask_wtf import FlaskForm
from wtforms.fields import StringField, PasswordField, IntegerField, SubmitField, BooleanField, IntegerRangeField, SelectField, FileField, TextAreaField
from wtforms.validators import DataRequired, IPAddress

modulesPath = os.path.join(pathlib.Path(__file__).resolve().parents[2], "modules")
sys.path.append(modulesPath)

from main_config.main_config import Config
from log_config.log_config import LoggingConf

# Initalize config
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

def create_form(config = None):

    config = config

    class MainForm(FlaskForm):
            
            logger.setLevel(config['logging']['level'])

            # S3 elements
            s3Config = config['s3']

            s3EndPointField = StringField(label = "S3 End Point", default = s3Config['endpoint'])
            s3BucketNameField = StringField(label = "Bucket Name", default = s3Config['bucketName'])
            s3KeyField = StringField(label = "Access Key",  default = s3Config['key'])
            s3SecretField = PasswordField(label = "Secret", default = s3Config['secret'])
            s3PageSizeField = IntegerField(label = "Page Size", default = int(s3Config['pageSize']))
            s3VerifyCertificateField = BooleanField(label="Verify Certificate")

            # Starburst elements
            starburstConfig = config['starburst']

            _typeChoices = ['DDAE', 'Enterprise', 'Galaxy']
            starburstTypeField = SelectField(label = "Deployment type", choices = _typeChoices, default = starburstConfig['type'])
            starburstHostField = StringField(label = "Host", default = starburstConfig['host'])
            starburstPortField = IntegerField(label = "Port",default = int(starburstConfig['port']))
            _schemeChoices = ["https", "http"]
            starburstHttpSchemeField = SelectField(label = "HTTP Scheme", choices = _schemeChoices, default = starburstConfig['httpScheme'])
            starburstVerifyCertificateField = BooleanField(label="Verify Certificate")

            
            starburstCatalogField = StringField(label = "Catalog", default = starburstConfig['catalog'])
            starburstUserField = StringField(label = "User", default = starburstConfig['user'])
            starburstRoleNameField = StringField(label = "Role", default = starburstConfig['roleName'])
            starburstPasswordField = PasswordField(label = "Password", default = starburstConfig['password'])

            starburstBucketField = StringField(label = "Bucket", default = starburstConfig['bucket'])
            starburstSchemaLocationField = StringField(label = "Schema path", default = starburstConfig['schemaLocation'])
            starburstSchemaField = StringField(label = "Schema", default = starburstConfig['schema'])
            starburstTableField = StringField(label = "Table", default = starburstConfig['table'])

            starburstGalaxyAccountDomainField = StringField(label = "Account domain", default = starburstConfig['galaxy']['accountDomain'])
            starburstGalaxyApiKeyField = StringField(label = "API Key", default = starburstConfig['galaxy']['apiKey'])
            starburstGalaxyApiSecretField = PasswordField(label = "API Secret", default = starburstConfig['galaxy']['apiSecret'])

            # Runtime elements

            runTimeConfig = config['runTime']
            loggingConfig = config['logging']
            visionAIConfig = config['visionAI']

            # These will be displayed under "Image Processing Settings" on the web GUI
            runTimeExtractExifField = BooleanField(label="Extract Exif data")
            runTimeExtractLabelsField = BooleanField(label="Object detection")
            runTimeSendToStarburstField = BooleanField(label="Upload metadata to Iceberg (Starburst)")

            # These will be displayed under "Runtime Settings"
            runTimeDryRunField = BooleanField(label="Dry run")
            runTimeIncrementalField = BooleanField(label="Incremental run")
            runTimePartialDownloadField = BooleanField(label="Partial downloads")
            runTimeTempPathField = StringField(label = "Temp download path", default = runTimeConfig['tempPath'])
            runTimeMaxNumberOfThreadsField = IntegerRangeField(label = f"Number of threads: { str(runTimeConfig['maxNumberOfThreads']) }", default = int(runTimeConfig['maxNumberOfThreads']))
            _loggingChoices = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
            loggingLevelField = SelectField(label="Logging level", choices = _loggingChoices, default = loggingConfig['level'])

            # This will be displayed under "Computer Vision Settings"
            visionAICredsField = TextAreaField(label = "Google Cloud Service Account Key (json string)", default = visionAIConfig['creds'])
            visionAICredsFilePathField = FileField(label = "Google Vision AI Credentials File", default = visionAIConfig['credsFilePath'])
            testButton = SubmitField(label = "Test")
            resetButton = SubmitField(label = "Reset")
            saveButton = SubmitField(label = "Save")
            runButton = SubmitField(label = "Run")

    form = MainForm()
    return form
 
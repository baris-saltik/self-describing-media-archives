import os, sys, pathlib, subprocess, logging, json, pprint, base64, requests
import logging.config
from requests.auth import HTTPBasicAuth, AuthBase
from urllib.parse import quote_plus

# Add modules to sys.path
sys.path.append(os.path.join(pathlib.Path(__file__).resolve().parents[2], 'modules'))

# Import additional custom modules
from main_config.main_config import Config
from log_config.log_config import LoggingConf

configObj = Config()

if __name__ == "__main__":
    configObj.update_main_conf()
    configObj.set_defaults()
    configObj.initialize_logging_conf()
    configObj.update_logging_conf()

config = configObj.mainConfigDict
starburstConfig = config['starburst']

# Set logging
loggingConfig = LoggingConf()
logging.config.dictConfig(loggingConfig.config)
logger = logging.getLogger(__name__)
# print(logger.handlers)
# print(logger)


# Custome bearor token authentication class
class BearerTokenAuth(AuthBase):
    """Attaches HTTP Pizza Authentication to the given Request object."""
    def __init__(self, bearerToken):
        self.bearerToken = bearerToken

    def __call__(self, r):
        # modify and return the request
        r.headers["Authorization"] = f"Bearer {self.bearerToken}"
        return r


class GalaxyAPI(object):

    def __init__(self, config = None ):

        config = config
        starburstConfig = config['starburst']
        logger.setLevel(config['logging']['level'])
        
        if not starburstConfig:
            logger.critical("No Starburst Galaxy credentials are not defined. Quiting...")
            sys.exit()
        
        starburstGalaxyConfig = starburstConfig["galaxy"]
        
        logger.info("#####" + " Started execution " + "###########")
        self.accountDomain = starburstGalaxyConfig['accountDomain']
        self.roleName = starburstConfig['roleName']
        self.key = starburstGalaxyConfig['apiKey']
        self.secret = starburstGalaxyConfig['apiSecret']
        self.bucket = starburstConfig['bucket']
        self.s3Location = f"s3://{self.bucket}/*"
        self.bearerToken = None
        self.sessionCreated = False
        self.locationPrivGranted = False
        self.verifyCertificate = starburstConfig['verifyCertificate']

    def create_session(self):
        _basicAuthString = base64.b64encode(f"{self.key}:{self.secret}".encode("ASCII")).decode("ASCII")
        _headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        _data = 'grant_type=client_credentials'
        _url = f"https://{self.accountDomain}/oauth/v2/token"

        self.session = requests.Session()
        self.session.verify = self.verifyCertificate
        # First set the session.auth to HTTP Basic Auth to get the bearer token
        self.session.auth = HTTPBasicAuth(username = self.key, password = self.secret)
        try:
            _response = self.session.post(headers = _headers, data = _data, url = _url)
            _response.raise_for_status()
            _content = _response.json()
            self.bearerToken = _content['access_token']
            logger.info(f"Bearer token has been acquired.")
            self.sessionCreated = True
        except Exception as err:
            logger.error(f"Bearer token could not be acquired!")
            logger.error(err)
            self.bearerToken = None
            self.sessionCreated = False

        # print(self.bearerToken)
    
    def get_role_id(self):
        _url = f"https://{self.accountDomain}/public/api/v1/role"
        _params = dict(pageSize = 100)
        self.session.auth = BearerTokenAuth(self.bearerToken)
        try:
            _response = self.session.get(params = _params, url = _url)
            _response.raise_for_status()
            pprint.pprint(_response.json())
            logger.info(f"RoleID has been acquired.")
        except Exception as err:
            logger.error(f"RoleId could not be acquired!")
            logger.error(err)
    
    def grant_location_privilege(self):
        _url = f"https://{self.accountDomain}/public/api/v1/role/name={quote_plus(self.roleName)}/privilege:grant"
        _headers = {"Content-Type": "application/json"}
        _params = dict(name=self.roleName)
        _data = {"grantKind": "Allow",
                 "entityId": self.s3Location,
                 "entityKind": "Location",
                 "grantOption": False,
                 "privilege": "CreateSql"}
        self.session.auth = BearerTokenAuth(self.bearerToken)
        try:
            _response = self.session.post(url = _url, json = _data)
            _response.raise_for_status()
            self.locationPrivGranted = True
            logger.info(f"Location privileges for {self.s3Location} granted to {self.roleName}")
        except Exception as err:
            self.locationPrivGranted = False
            logger.critical(f"Location privileges for {self.s3Location} cannot be granted to {self.roleName}!")
            logger.debug(f'{_response.content.decode("UTF-8")}')
        
    def list_role_privileges(self):
        _url = f"https://{self.accountDomain}/public/api/v1/role/name={quote_plus(self.roleName)}/privilege"
        _params = dict(pageSize = 100)
        self.session.auth = BearerTokenAuth(self.bearerToken)
        _response = self.session.get(url = _url, params = _params)
        pprint.pprint(_response.json())
        _response.raise_for_status()

    def __exit__(self):
        logger.info("##########" + " Ended execution " + "###########")


if __name__ == '__main__':

    galaxyAPI = GalaxyAPI(config = config )
    galaxyAPI.create_session()
    # galaxyAPI.get_role_id()
    galaxyAPI.grant_location_privilege()
    # galaxyAPI.list_role_privileges()
    galaxyAPI.__exit__()
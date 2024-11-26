import os, sys, pathlib, subprocess, logging, json, pprint, re
import logging.config
# PyStarburst packages
import trino
from pystarburst import Session
from pystarburst.types import IntegerType, StringType, TimestampType, TimestampNTZType, DateType, ArrayType, StructField, StructType

# Add modules to sys.path
sys.path.append(os.path.join(pathlib.Path(__file__).resolve().parents[2], 'modules'))

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

class StarburstData(object):

    def __init__(self, config = None ):

        config = config
        starburstConfig = config['starburst']
        runtimeConfig = config['runTime']
        # Set logging
        loggingConfig = LoggingConf()
        logging.config.dictConfig(loggingConfig.config)
        logger = logging.getLogger(__name__)
        logger.setLevel(config['logging']['level'])

        if not starburstConfig:
            logger.critical("No Starburst credentials are defined. Quiting...")
            sys.exit()
        
        logger.info("#####" + " Started execution " + "###########")

        self.clusterAvailable = False
        self.schemaExists = False
        self.tableExists = False
        self.tableCreated = False
        self.starburstConfig = starburstConfig
        self.dbParameters = {}
        self.dbParameters['host'] = starburstConfig['host']
        self.dbParameters['port'] = starburstConfig['port']
        # Keys are different!
        self.dbParameters['http_scheme'] = starburstConfig['httpScheme']
        # self.dbParameters['catalog'] = starburstConfig['catalog']
        # self.dbParameters['schema'] = starburstConfig['schema']
        self.dbParameters['auth'] = trino.auth.BasicAuthentication(starburstConfig['user'], starburstConfig['password'])
        self.dbParameters['roles'] = starburstConfig['roleName']
        self.dbParameters['verify'] = starburstConfig['verifyCertificate']

        self.catalog = self.starburstConfig['catalog']
        self.schema = self.starburstConfig['schema']
        self.bucket = self.starburstConfig['bucket']
        self.schemaLocation = self.starburstConfig['schemaLocation']
        self.table = self.starburstConfig['table']
        self.columns = self.starburstConfig['columns']
         # createdate: "2020:07:17 09:10:57"
        self.dateTimeRegEx = re.compile(r"(\d+):(\d+):(\d+)\s+(.*)")
        self.schemaConstruct = None
   
    # create_session
    def create_session(self):

        try:
            logger.info("Creating a session...")
            _session = Session.builder.configs(self.dbParameters).create()
            logger.info("Successfuly connected to the Starburst/DDAE cluster.")
        except Exception as err:
            logger.error(err)
            self.clusterAvailable = False
            logger.critical(f"Could not connect to {self.dbParameters['host']}. Quiting...")
            return False
        
        try:
            _response = _session.sql("SELECT * FROM system.runtime.nodes").collect()
        except Exception as err:
            self.clusterAvailable = False
            logger.critical("Could not connect to Starburs/DDAE cluster. Quiting...")
            logger.critical(err)
            return False

        logger.info(" Starburst/DDAE Nodes: ".center(60, "="))

        for item in _response:
            logger.info("-" * 60)
            logger.info(f"Node: {item.node_id}")
            logger.info(f"Version: {item.node_version}")
            logger.info(f"Coordinator: {item.coordinator}")
            logger.info(f"State: {item.state}")
            if item.coordinator and item.state == "active":
                self.clusterAvailable = True
    
        logger.info("-" * 60)
        self.session = _session
    
    # verify_schema
    def verify_schema(self):

        # Create the schema if not exists
        if not self.clusterAvailable:
            schemaExists = False
            return False
        
        try:
            _response = self.session.sql(f"SHOW SCHEMAS FROM {self.catalog}").collect()
        except Exception as err:
            logger.critical("Could not connect to Starburs/DDAE cluster.")
            logger.critical(err)
            return False

        for row in _response:
            if row.Schema.lower() == self.schema:
                self.schemaExists = True
                break
    
    # create_schema
    def create_schema(self):

        # Create the schema if not exists
        if not self.clusterAvailable:
            self.schemaExists = False
            return False
        
        if not self.schemaExists:
            _sql = f"""CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}
                    WITH (
                        location = 's3a://{self.bucket}{self.schemaLocation}'
                    )
                    """
            try:
                self.session.sql(f"{_sql}").collect()
                self.schemaExists = True
            except Exception as err:
                logger.critical(f"Could not create {self.schema}!")
                logger.critical(err)
        else:
            logger.info(f"Schema {self.schema} exist already.")
    
    # verify_table
    def verify_table(self):

        if not self.schemaExists:
            logger.critical(f"Schema {self.schema} does not exist!")
            return False
        
        try:
            _sqlString = f"""SHOW TABLES FROM {self.catalog}.{self.schema}"""
            _response = self.session.sql(f"{_sqlString}").collect()
            # print(type(_response))
            # print(_response)
            if _response:
                for item in _response:
                    if item.Table == self.table:
                        self.tableExists = True
                        logger.info(f"Table {self.table} verified.")
                        break
            else:
                self.tableExists = False           
        except Exception as err:
            self.tableExists = False
            logger.critical(f"Table {self.table} does not exist!")
            return False
    
    # delete_schema
    def delete_table(self):
        if not self.schemaExists:
            logger.critical(f"Schema {self.schema} does not exist!")
            return False
    
        try:
            _table = self.session.table(f'"{self.catalog}"."{self.schema}"."{self.table}"')
            _table.drop_table()
        except Exception as err:
            logger.error(f"Table {self.table} could not be deleted!")
            logger.error(err)
        
        try:
            _table.collect()
            self.tableExists = True
        except Exception as err:
            logger.info(f"Table {self.table} was deleted.")
            self.tableExists = False

    # create_table
    def create_table(self):

        if not self.schemaExists:
            logger.critical(f"Schema {self.schema} does not exist!")
            return False
        
        # Make sure table has been deleted
        self.delete_table()
        if self.tableExists:
            logger.critical(f"Table {self.table} could not be deleted. Disabling Starburst/DDAE updates!")
            return False
        
        try:
            # _columns = [f"{_column} VARCHAR" if not _column == "createdate" else f"{_column} TIMESTAMP" for _column in self.columns]
            # _columnString = ','.join(_columns)
            _schema = [StructField(column_identifier = _col, datatype = StringType(), ) if not _col == "createdate" else StructField(column_identifier = _col, datatype = TimestampNTZType()) for _col in self.columns]
            
            ''' _sqlString = f"""CREATE OR REPLACE TABLE {self.catalog}.{self.schema}.{self.table} ({_columnString})
                                         WITH (
                                                format = 'PARQUET',
                                                format_version = 2,
                                                location = 's3://{self.bucket}/{self.schema}/{self.table}',
                                                sorted_by = ARRAY['createdate DESC']
                                         )
                                         """
            '''

            df = self.session.create_dataframe(data = [], schema = StructType(fields = _schema))
            df.write.save_as_table(table_name = f"{self.catalog}.{self.schema}.{self.table}", mode = "overwrite", 
                                   table_properties = {"format": "PARQUET", 
                                                       "location": f"s3://{self.bucket}/{self.schema}/{self.table}",
                                                       "sorted_by": ['createdate DESC']
                                                       }
                                    )               

            # print(_sqlString)
            # _response = self.session.sql(f"{_sqlString}").collect()
        
            logger.info(f"Table {self.table} has been created.")
            self.tableCreated = True
            self.tableExists = True
            self.schema
        except Exception as err:
            self.tableExists = False
            logger.critical(f"Table {self.table} could not be created!")
            logger.debug(err)
            return False

    # insert_into_table
    def insert_into_table(self, metadata = None):

        if not self.tableExists:
            logger.critical(f"Table {self.schema} does not exist!")
            return False
        
        if not metadata:
            logger.critical("Did not receive any metadata records to injest into the table.")
            return False

        try:
            _columns = self.columns
            # _schema = [StructField(column_identifier = _col, datatype = StringType()) if not _col == "createdate" else StructField(column_identifier = _col, datatype = TimestampNTZType()) for _col in self.starburstConfig['columns']]
            # _schema = [StructField(column_identifier = _col, datatype = StringType()) for _col in self.starburstConfig['columns']]
            _data = []

            # * metadata is a list of dictionaries
            # * _obj is a dictionary
            # * _data is a list of lists

            # logger.debug(f"######### metadata ########### {metadata}")
            # logger.debug(f"######### columns ############ {_columns}")

            for _obj in metadata:
                _objRow = []
                 # Extract date and time columns
                 # createdate: "2020:07:17 09:10:57"
                _timeStamp = None
                if "createdate" in _obj:
                    mo = self.dateTimeRegEx.search(_obj["createdate"])
                    if mo:
                        _timeStamp = f"{mo.group(1)}-{mo.group(2)}-{mo.group(3)} {mo.group(4)}"
                
                if _timeStamp:
                    if "0000" in _timeStamp:
                        _timeStamp = '0001-01-01 00:00:00'
            
                for _col in _columns:
                    _obj.setdefault(_col, None)
                    # Set create date
                    if _col == "createdate":
                        if _timeStamp:
                            _obj[_col] = _timeStamp
                        else:
                            _obj[_col] = '0001-01-01 00:00:00'
                    
                    _objRow.append(_obj[_col])
                _data.append(_objRow)
                            
            _schema = [StructField(column_identifier = _col, datatype = StringType(), ) if not _col == "createdate" else StructField(column_identifier = _col, datatype = TimestampNTZType()) for _col in self.columns]
            
            # logger.debug(f"#####################: {_data}")
            # logger.debug(_schema)

            df = self.session.create_dataframe(data = _data, schema = StructType(_schema),)
            df.write.save_as_table(table_name = f"{self.catalog}.{self.schema}.{self.table}",
                                   mode = "append", column_order = "index",)
        
            logger.info(f"Records have been inserted into table: {self.table}.")

        except Exception as err:
            logger.error(f"Records could not be inserted into table: {self.table}!")
            logger.error(err)
            return False
        
        return True


        # Create the schema 
        # schema = StructType([StructField("a", IntegerType()), StructField("b", StringType())]) 
        # _schema = [] 
        # [_schema.append(StructField(column, StringType())) for column in self.columns]
        
    # __exit__
    def __exit__(self):
        logger.info("##########" + " Ended execution " + "###########")


if __name__ == '__main__':

    starburstConfig = config['starburst']
    runtimeConfig = config['runTime']

    schema = starburstConfig['schema']
    table = starburstConfig['table']
    incremental = runtimeConfig['incremental']
    sendToStarburst = runtimeConfig['sendToStarburst']

    # Send to Starburst
    if not sendToStarburst:
        logger.info(f"Metadata will not be sent to Starburst/DDAE.")
        sys.exit()


    starburst = StarburstData(config = config)
    starburst.create_session()
    if starburst.clusterAvailable:
        logger.info(f"Starburst/DDAE Cluster is active")
    else:
        logger.error(f"Starburst/DDAE Cluster is not active!")
    
    # Verify Schema
    starburst.verify_schema()

    # Create schema checks whether the schema exists or not
    if not incremental: starburst.create_schema()
    
    if not starburst.schemaExists:
        logger.critical(f"Schema {schema} could not be created!")
        starburst.__exit__()
        sys.exit()

    # Incremental run
    if incremental:
        starburst.verify_table()
        if starburst.tableExists:
            logger.info(f"Table {starburst.table} exist.")
        else:
            starburst.__exit__()
            logger.critical(f"Table {starburst.table} does not exist! Quiting...")
            sys.exit()
    # Full run
    else:
        starburst.create_table()
        if not starburst.tableExists:
            starburst.__exit__()
            logger.critical(f"Table {table} could not be created!. Quiting...")
            sys.exit()

    # Insert the records
    # starburst.insert_into_table()

    starburst.__exit__()
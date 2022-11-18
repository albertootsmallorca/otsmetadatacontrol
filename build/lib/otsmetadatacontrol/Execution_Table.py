from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import *
from pyspark.sql import *

import datetime

class Execution_Table:
    def __init__(self, id_execution, id_process, status):
        self.id_execution = id_execution
        self.id_process = id_process
        self.status = status
    
    def to_print (self):
        print ("id_execution: " + str(self.id_execution))
        print ("id_process: " + str (self.id_process))
        print ("status: " + self.status)
    
    def get_data (self):
        data = [(self.id_execution, self.id_process, datetime.datetime.today(), self.status)]
        return data
    
    def get_schema (self):
        schema = StructType([
            StructField("ID_EXECUTION",IntegerType(),True),
            StructField("ID_PROCESS",IntegerType(),True),
            StructField("EVENT_TIME",TimestampType(),True), 
            StructField("STATUS", StringType(), True)])

        return schema

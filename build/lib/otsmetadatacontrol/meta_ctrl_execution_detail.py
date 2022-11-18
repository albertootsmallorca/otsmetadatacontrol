############################################################################################
# Fecha creación: 06/05/2022
# Empresa: Inetum
# Autor: Jose Luis Garcia Iranzo
#
# Descripción:
# Clase que representa a la tabla metadata.ctrl_execution_detail. Deriva de la clase db_connection, 
# con lo que se consigue heredar funciones para establecer la conexión.
#
# Listado de Atributos
#  id_execution: Identificación de la ejecución correspondiente a ctrl_execution
#  id_process: Identificador del proceso correspondiente a ctrl_process
#  start_date: Fecha de inicio de la ejecución
#  end_date: Fecha de finalización de la ejecución
#  status: Estado de la ejecución. Permite tener los estados "STARTED", "FINISHED", "STOPPED", "ERROR"
#  creation_date: Fecha de inserción del registro en la base de datos
#
# Funciones públicas:
#  set_new_execution: Crea un nuevo registro en la base de datos si no hay ejecuciones pendientes para el proceso
#  set_update_execution: Actualiza el estado de la ejecución con el parámetro que se le pasa
############################################################################################

# Librerías
import random 

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import *
from pyspark.sql import *
from otsmetadatacontrol.db_connection import db_connection
from datetime import datetime
from time import sleep


class meta_ctrl_execution_detail(db_connection):
    # Atributos
    id_execution = ""
    id_process = -1
    start_date = datetime.today()
    end_date = datetime.today()
    status = ""
    creation_date = datetime.today()

    # Constructor
    def __init__(self):
        self.id_execution = ""
        self.id_process = -1
        self.start_date = datetime.today()
        self.end_date = datetime.today()
        self.status = ""
        self.creation_date = datetime.today()

    # Función privada
    # Obtiene la ejecución siempre y cuando el proceso no haya sido marcado como finalizado. Devuelve el id_proceso y en caso de no encontrarlo devuelve "-1"
    def __get_current_execution(self, id_execution, id_process):
        spark = self.get_spark()
        df = spark.table("metadata.ctrl_execution_detail").filter(
            'END_DATE is NULL AND ID_EXECUTION = "' + id_execution + '" AND ID_PROCESS = ' + str(id_process))

        if df.count() > 0:
            aux = df.collect()
            self.id_execution = id_execution
            self.id_process = id_process
            self.start_date = aux[0][2]
            self.end_date = aux[0][3]
            self.status = aux[0][4]
            self.creation_date = aux[0][5]

            return self.id_execution
        else:
            return "-1"

    # Función privada
    # Obtiene el identificador del proceso sin tener en cuenta si esta en ejecución. Devuelve -1 si no lo encuentra, en caso contrario devuelve el id_execution del registro
    def __get_execution(self, id_execution, id_process):
        spark = self.get_spark()
        df = spark.table("metadata.ctrl_execution_detail").filter(
            'id_process  = ' + str(id_process) + ' and id_execution = "' + id_execution + '"')

        if df.count() > 0:
            aux = df.collect()
            self.id_execution = id_execution
            self.id_process = id_process
            self.start_date = aux[0][2]
            self.end_date = aux[0][3]
            self.status = aux[0][4]
            self.creation_date = aux[0][5]

            return self.id_execution
        else:
            return "-1"

    #Funcion publica
    #Finaliza un proceso cuyo end_date esta a null, es decir, sin finalizar, para no tener que hacerlo manualmente
    def finish_started(self, id_execution, status):
        spark = self.get_spark()
        
        if self.end_date == "null":
            spark.sql('UPDATE metadata.ctrl_execution set end_date = current_timestamp (), status = "' + status + '" where id_execution = "' + id_execution + '"')
            spark.sql('UPDATE metadata.ctrl_execution_detail set end_date = current_timestamp (), status = "' + status + '" where id_execution = "' + id_execution + '"')
        
        return 0

    # Función públicas
    # Crea un nuevo registro en la tabla siempre y cuando no haya una ejecución sin cerrar para un mismo proyecto.
    def set_new_execution(self, id_execution, id_process):
        spark = self.get_spark()

        id = self.__get_execution(id_execution, id_process)

        if (id is None or id == "-1") and id_execution != "-1" and id_process != "-1":
            # No hay ejecuciones previas y se puede crear una nueva ejecución
            self.id_execution = id_execution
            self.id_process = id_process
            self.start_date = datetime.today()
            self.end_date = None
            self.status = "STARTED"
            self.creation_date = datetime.today()

            data = [
                (self.id_execution, self.id_process, self.start_date, self.end_date, self.status, self.creation_date)]
            schema = StructType([
                StructField("ID_EXECUTION", StringType(), True),
                StructField("ID_PROCESS", IntegerType(), True),
                StructField("START_DATE", TimestampType(), True),
                StructField("END_DATE", TimestampType(), True),
                StructField("STATUS", StringType(), True),
                StructField("CREATION_DATE", TimestampType(), True)])

            df = spark.createDataFrame(data, schema)
            df.write.format("delta").mode("append").saveAsTable("metadata.ctrl_execution_detail")

            return self.id_execution
        else:
            # raise Exception("No es posible crear una nueva ejecución porque ya se ha realizado una ejecución previa")
            self.finish_started(id_execution, "STOPPED")

	#Función públicas
    #Actualiza el estado del registro con el estado que se pasa por parametro. 
    def set_update_execution (self, id_execution, id_process, status):
        spark = self.get_spark()
        
        if self.__get_execution (id_execution, id_process) == "-1":
            raise Exception("No se ha encontrado el registro")
        
        self.status = status
        
        spark.sql('UPDATE metadata.ctrl_execution_detail set end_date = CURRENT_TIMESTAMP(), status = "' + status + '" where id_execution = "' + id_execution + '" and id_process = ' + str (id_process))

        return 0

############################################################################################
# Fecha creación: 06/05/2022
# Empresa: Inetum
# Autor: Jose Luis Garcia Iranzo
#
# Descripción:
# Clase que representa a la tabla metadata.ctrl_execution_log. Deriva de la clase db_connection, 
# con lo que se consigue heredar funciones para establecer la conexión.
#
# Listado de Atributos
#  id_execution: Identificación de la ejecución correspondiente a ctrl_execution
#  id_process: Identificador del proceso correspondiente a ctrl_process
#  event_date: Fecha de creación del evento
#  message: mensaje a guardar
#  type: Indicar el tipo de mensaje
#  creation_date: Fecha de inserción del registro en la base de datos
#
# Funciones públicas:
#  write: Crea un nuevo registro en la base de datos
############################################################################################

#Librerías
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import *
from pyspark.sql import *
from otsmetadatacontrol.db_connection import db_connection
from datetime import datetime

class meta_ctrl_execution_log(db_connection):
	#Atributos
	id_execution = ""
	id_process = ""
	event_date = datetime.today()
	message = ""
	type = ""
	creation_date = datetime.today()

	#Función públicas
	#Función que permite escribir el log en la tabla de bbdd
	def write (self, id_execution, id_process, message, type):
		self.id_execution = id_execution
		self.id_process = id_process
		self.message = message
		self.type = type
		self.creation_date = datetime.today()
		
		data = [(self.id_execution, self.id_process, self.event_date, self.message, self.type, self.creation_date)]
		schema = StructType([
			StructField("ID_EXECUTION",StringType(),True),
			StructField("ID_PROCESS",IntegerType(),True),
			StructField("EVENT_DATE",TimestampType(),True),
			StructField("MESSAGE",StringType(),True),
			StructField("TYPE",StringType(),True), 
			StructField("CREATION_DATE",TimestampType(),True)])
	
		spark = self.get_spark()
		df = spark.createDataFrame(data, schema)
		df.write.format("delta").mode("append").saveAsTable("metadata.ctrl_execution_log")
		
		return 0
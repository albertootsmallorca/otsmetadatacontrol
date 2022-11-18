############################################################################################
# Fecha creación: 06/05/2022
# Empresa: Inetum
# Autor: Jose Luis Garcia Iranzo
#
# Descripción:
# Clase que representa a la tabla metadata.ctrl_process. Deriva de la calse db_connection, 
# con lo que se consigue heredar funciones para establecer la conexión.
#
# Listado de Atributos
#  id_process: Identificación del proceso
#  project_name: Nombre del proyecto
#  pipeline_name: Nombre del pipeline en Datafactory
#  process_name: Nombre del script a ejecutar
#  active: 1 indica que esta activo el proceso (que se puede ejecutar) y 0 que no.  
#  creation_date: Fecha de inserción del registro en la base de datos
#
# Funciones públicas:
#  get_id_process: Proporciona el id (int) del proceso que lo identifica de forma únivoca
############################################################################################

#Librerias necesarias
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import *
from pyspark.sql import *
from otsmetadatacontrol.db_connection import db_connection
from otsmetadatacontrol.db_error import db_error
from datetime import datetime

class meta_ctrl_process (db_connection):
	#Atributos
	id_process = -1
	project_name = ""
	pipeline_name = ""
	process_name = ""
	active = 0
	creation_date = datetime.today()
	
	#Constructor de la clase
	def __init__(self):
		self.id_process = -1
		self.project_name = ""
		self.pipeline_name = ""
		self.process_name = ""
		self.active = 0
		self.creation_date = datetime.today()

	# Función privada
	# Permite obtener el registro si lo encuentra en la base de datos. Devuelve el id_process si lo encuentra. En caso contrario devuelve -1
	def __get_process (self, project_name, pipeline_name, process_name):
		spark = self.get_spark()
		df = spark.table("metadata.ctrl_process").filter ('project_name = "' + project_name + '" and pipeline_name = "' + pipeline_name + '" and process_name = "' + process_name + '"')
	
		if df.count() > 0: 
			aux = df.collect()
			self.id_process = aux [0][0]
			self.project_name = aux [0][1]
			self.pipeline_name = aux [0][2]
			self.process_name = aux [0][3]
			self.active = aux [0][4]
			self.creation_date = aux [0][5]
						
			return self.id_process
		else:
			return -1
	
	# Función pública
	# Obtiene el identificador del proceso. Si el proceso existe en la base de datos y esta activo devolverá el id_process. En caso contrario devolverá una excepción.
	def get_id_process (self, project_name, pipeline_name, process_name):
		id = self.__get_process (project_name, pipeline_name, process_name)
		
		if self.active == 1:
			return id
		else:
			raise Exception ("El proceso no esta activo")
	
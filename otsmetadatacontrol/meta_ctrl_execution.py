############################################################################################
# Fecha creación: 06/05/2022
# Empresa: Inetum
# Autor: Jose Luis Garcia Iranzo
#
# Descripción:
# Clase que representa a la tabla metadata.ctrl_execution. Deriva de la clase db_connection, 
# con lo que se consigue heredar funciones para establecer la conexión.
#
# Listado de Atributos
#  id_execution: Identificación de la ejecución
#  project_name: Nombre del proyecto
#  start_date: Fecha de inicio de la ejecución
#  end_date: Fecha de finalización de la ejecución
#  status: Estado de la ejecución. Permite tener los estados "STARTED", "FINISHED", "STOPPED", "ERROR"
#  creation_date: Fecha de inserción del registro en la base de datos
#
# Funciones públicas:
#  set_new_execution: Crea un nuevo registro en la base de datos si no hay ejecuciones pendientes para el proyecto
#  set_end_execution: Finaliza la ejecución siempre y cuando no tenga procesos en ejecución
#  get_current_execution: Función que permite a los procesos hijos saber la ejecución en curso para el proyecto
############################################################################################

#Librerias necesarias
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import *
from pyspark.sql import *
from otsmetadatacontrol.db_connection import db_connection
from datetime import datetime

class meta_ctrl_execution (db_connection):
	#Atributos
	id_execution = "-1"
	project_name = ""
	start_date = datetime.today()
	end_date = datetime.today()
	status = ""
	creation_date = datetime.today()
	
	#Constructor
	def __init__(self):
		self.id_execution = "-1"
		self.project_name = ""
		self.start_date = datetime.today()
		self.end_date = datetime.today()
		self.status = ""
		self.creation_date = datetime.today()
	
	#Función privada
	#Genera el identificador de la ejecución que luego arrastrarán todos los procesos de un mismo proyecto
	def __create_id (self, text):
		return text + ":" + datetime.today().strftime("%Y%m%d-%H%M%S%f")

	#Función privada
	#Obtiene el registro de la base de datos buscando por nombre de proyecto y siempre y cuando este en ejecución. Devuelve el identificador de la ejecución y en caso de que no haya nada en ejecución devolverá "-1"
	def get_current_execution (self, project_name):
		spark = self.get_spark()
		df = spark.table("metadata.ctrl_execution").filter ('END_DATE is NULL AND PROJECT_NAME = "' + project_name + '"')
		
		if df.count() > 0: 
			aux = df.collect()
			self.id_execution = aux[0][0]
			self.project_name = project_name
			self.start_date = aux[0][2]
			self.end_date = aux[0][3]
			self.status = aux[0][4]
			self.creation_date = aux[0][5]
			
			return self.id_execution
		else:
			return "-1"
	
	#Función privada
	#Obtiene el registro de la base de datos por el id_execution. Devuelve el identificador de la ejecución y en caso de que no haya nada en ejecución devolverá "-1" 
	def __get_execution (self, id_execution):
		spark = self.get_spark()
		df = spark.table("metadata.ctrl_execution").filter ('id_execution  = "' + str (id_execution) + '"')
		
		if df.count() > 0: 
			aux = df.collect()
			self.id_execution = id_execution
			self.project_name = aux[0][1]
			self.start_date = aux[0][2]
			self.end_date = aux[0][3]
			self.status = aux[0][4]
			self.creation_date = aux[0][5]
			
			return self.id_execution
		else:
			return "-1"
	
	#Función privada
	#Obtiene el id_ejecución más alto para un proyecto. En caso de que no haya ejecuciones devuelve None
	def __get_max_execution (self, project_name):
		spark = self.get_spark()
		df = spark.table("metadata.ctrl_execution").filter ('PROJECT_NAME = "' + project_name + '"').agg({"ID_EXECUTION": "max"})
		
		if df.count() > 0: 
			aux = df.collect()
			self.id_execution = aux[0][0]
						
			return self.id_execution
		else:
			return None

	#Funcion publica
	#Finaliza un proceso cuyo end_date esta a null, es decir, sin finalizar, para no tener que hacerlo manualmente
	def finish_started(self, id_execution, status):
		spark = self.get_spark()

		if self.end_date == "null":
			spark.sql('UPDATE metadata.ctrl_execution set end_date = current_timestamp (), status = "' + status + '" where id_execution = "' + id_execution + '"')
			spark.sql('UPDATE metadata.ctrl_execution_detail set end_date = current_timestamp (), status = "' + status + '" where id_execution = "' + id_execution + '"')

		return 0

	#Función pública
	#Registra una nueva ejecución en la tabla siempre y cuando no exista una ejecución previa sin finalizar. En caso contrario devuelve una excepción		
	def set_new_execution (self, project_name):
		spark = self.get_spark()
		
		id_execution = self.__get_max_execution (project_name)
		if id_execution is None:
			#No hay ejecuciones previas y se puede crear una nueva ejecución
			self.id_execution = self.__create_id (project_name)
			self.project_name = project_name
			self.start_date = datetime.today()
			self.end_date = None
			self.status = "STARTED"
			self.creation_date = datetime.today()
			
			data = [(self.id_execution, self.project_name, self.start_date, self.end_date, self.status, self.creation_date)]
			schema = StructType([
				StructField("ID_EXECUTION",StringType(),True),
				StructField("PROJECT_NAME",StringType(),True),
				StructField("START_DATE",TimestampType(),True),
				StructField("END_DATE",TimestampType(),True),
				StructField("STATUS",StringType(),True), 
				StructField("CREATION_DATE",TimestampType(),True)])
		
			df = spark.createDataFrame(data, schema)
			df.write.format("delta").mode("append").saveAsTable("metadata.ctrl_execution")
			
			return self.id_execution
		else:
			self.__get_execution (id_execution)  
			
			if self.end_date is None:
				# Existe una ejecución y por lo tanto no se puede crear una nueva ejecución.
				# raise Exception("No es posible crear una nueva ejecución porque ya hay una ejecución en curso")
				self.finish_started(id_execution, "STOPPED")

			if self.status == "STOPPED" or self.status == "FINISHED":
				#No hay ejecuciones previas y se puede crear una nueva ejecución
				self.id_execution = self.__create_id (project_name)
				self.project_name = project_name
				self.start_date = datetime.today()
				self.end_date = None
				self.status = "STARTED"
				self.creation_date = datetime.today()
				
				data = [(self.id_execution, self.project_name, self.start_date, self.end_date, self.status, self.creation_date)]
				schema = StructType([
					StructField("ID_EXECUTION",StringType(),True),
					StructField("PROJECT_NAME",StringType(),True),
					StructField("START_DATE",TimestampType(),True),
					StructField("END_DATE",TimestampType(),True),
					StructField("STATUS",StringType(),True), 
					StructField("CREATION_DATE",TimestampType(),True)])
			
				df = spark.createDataFrame(data, schema)
				df.write.format("delta").mode("append").saveAsTable("metadata.ctrl_execution")
				return self.id_execution
	
	#Función privada
	#Actualiza el estado de la ejecución modificando la fecha end_date y el estado. Devuelve 0 si funciona correctamente
	def __set_update_execution (self, id_execution, status):
		spark = self.get_spark()
		
		if self.__get_execution (id_execution) == "-1":
			raise Exception("No se ha encontrado el registro")
		
		self.status = status
		
		spark.sql('UPDATE metadata.ctrl_execution set end_date = CURRENT_TIMESTAMP(), status = "' + status + '" where id_execution = "' + id_execution + '"')		
				
		return 0
	
	#Función privada
	#Devuelve el número de ejecuciones pendientes en la tabla ctrl_execution_detail relacionada con la ejecución id_execution
	def __get_num_ejecuciones_pendientes (self, id_execution):
		spark = self.get_spark()
		return spark.table("metadata.ctrl_execution_detail").filter ('id_execution = "' + id_execution + '" and status == "STARTED"').count()
	
	#Función pública
	#Finaliza una ejecución siempre y cuando  no tenga ejecuciones pendientes. Al finalizar marcará el registro con el estado FINISHED.
	def set_end_execution (self, id_execution):
		self.__get_execution (id_execution)
		
		if self.id_execution == "-1":
			raise Exception ("No se ha encontrado la ejecución")
		else:
			if self.end_date is not None or self.status != "STARTED":
				raise Exception ("No se puede finalziar la ejecución porque la tarea tiene un estado incosistente")
			else:
				if self.__get_num_ejecuciones_pendientes (id_execution) > 0:
					raise Exception ("Hay ejecuciones pendientes y no se puede cerrar la ejecución")
				else:
					self.__set_update_execution (id_execution, "FINISHED")
		
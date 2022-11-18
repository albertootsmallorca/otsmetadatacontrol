############################################################################################
# Fecha creación: 06/05/2022
# Empresa: Inetum
# Autor: Jose Luis Garcia Iranzo
#
# Descripción:
# Clase que contiene funciones para devolver parámetros de configuración del sistema.
#
# Listado de Atributos
#  No contiene atributos
#
# Funciones públicas:
#  get_hostname: Proporciona el hostname de la bbdd
#  get_port: Proporciona el puerto de la bbdd
#  get_database_name: Proporciona el nombre de la bbdd 
############################################################################################

#Librerias necesarias
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils


class db_configuration:
	spark = None
	
	def get_key_vault (self):
		return "key_vault_secret"
		
	def get_hostname (self):
		dbutils = self.get_dbutils()
		return dbutils.secrets.get(scope = self.get_key_vault(), key = "dbhostname")
	def get_port (self):
		dbutils = self.get_dbutils()
		return dbutils.secrets.get(scope = self.get_key_vault(), key = "dbport")
		
	def get_database_name (self):
		dbutils = self.get_dbutils()
		return dbutils.secrets.get(scope = self.get_key_vault(), key = "dbname")
		
	def get_blob_storage_name_secret (self):
		dbutils = self.get_dbutils()
		return dbutils.secrets.get(scope = self.get_key_vault(), key = "datablobstorage")
	
	def get_blob_storage_secret_pass (self):
		dbutils = self.get_dbutils()
		return dbutils.secrets.get(scope = self.get_key_vault(), key = "datablobstorage-key")		
	
	def get_blob_storage_raw_container (self):
		dbutils = self.get_dbutils()
		return dbutils.secrets.get(scope = self.get_key_vault(), key = "datablobstoragecontainer")	
		
	
	#Obtiene la sesión de spark en caso de que no se haya establecido en la clase
	def get_spark (self):
		if self.spark is None:
			self.spark = SparkSession.builder.getOrCreate()
		return self.spark
	
	#Establece la sesión de spark
	def set_spark (spark):
		self.spark = spark
	
	#Permite obtener las librerias dbutils para su posterior uso
	def get_dbutils(self):
		spark = self.get_spark()
		if spark.conf.get("spark.databricks.service.client.enabled") == "true":
			from pyspark.dbutils import DBUtils
			return DBUtils(spark)
		else:
			import IPython
			return IPython.get_ipython().user_ns["dbutils"]
############################################################################################
# Fecha creación: 06/05/2022
# Empresa: Inetum
# Autor: Jose Luis Garcia Iranzo
#
# Fichero: db_connection.pyspark
#
# Descripción:
# Clase que representa a una conexión ya sea de base de datos o a deltalake.
#
# Listado de Atributos
#  spark: Sesión de spark
#  
# Funciones públicas:
# get_database_url: Obtiene la url de la base de datos en formato jdbc
# get_database_connection_properties: Proporciena un array con las propiedades de la conexión necesaria
#                                     para conectarse a la base de datos
# get_database_user: Obtiene el nombre de usuario para conectarse a la base de datos
# get_database_password: Obtiene el password para conectarse a la base de datos
# get_database_driver: Obtiene el nombre del driver jdbc necesario para conectarse a la base de datos
# get_spark: Permite obtener la sesión de spark
# set_spark: Permite establecer en la clase el objeto spark.
# get_dbutils: Permite obtener las librerias dbutils para su uso.
############################################################################################

#Librerias necesarias
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from otsmetadatacontrol.db_configuration import db_configuration


class db_connection:
	spark = None
	
	#Genera la cadena de conexión jdbc, en base a los parametros definidos en la clase db_configuration
	def get_database_url (self):
		c = db_configuration ()
		url = "jdbc:sqlserver://{0}:{1};database={2}".format (c.get_hostname(), c.get_port(), c.get_database_name())
		return url
	
	#Obtiene la cadena de conexión necesaria para conectarse a la base de datos
	def get_database_connection_properties (self):
		dbutils = self.get_dbutils()
		connectionProperties = {\
			"user" : "APP_Databricks",\
			"password" : dbutils.secrets.get(scope = "key_vault_secret", key = "dbdatabrickspass") ,\
			"driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"\
		}
		return connectionProperties
	
	#Obtiene el usuario de base de datos
	def get_database_user (self):
		return "APP_Databricks"

	#Obtiene el password de la base de datos
	def get_database_password (self):
		return dbutils.secrets.get(scope = "key_vault_secret", key = "dbdatabrickspass")
	
	#Obtiene el driver jdbc necesario para conectarsee a la bases de datos
	def get_database_driver(self):
		return "com.microsoft.sqlserver.jdbc.SQLServerDriver"

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
    
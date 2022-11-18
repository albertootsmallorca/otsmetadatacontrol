class db_error:
	code = 0
	message = ""
	query = ""
	type = ""
	
	def __init__(self):
		self.code = 0
		self.type = ""
		self.message = ""
		self.query = ""
		self.error = None
	
	def __init__(self, error):
		self.code = 0
		self.type = ""
		self.message = ""
		self.query = ""
		self.error = error
		
	def set_database_error (self, code, message, query):
		self.code = code
		self.message = message
		self.query = query
		self.type = "DATABASE"
		
	def set_control_error (self, code, message, query):
		self.code = code
		self.message = message
		self.query = query
		self.type = "CONTROL"
	
	def get_code (self):
		return self.code
	
	def get_type (self):
		return self.type
	
	def get_message (self):
		return self.message

	def get_query (self):
		return self.query
		
	def to_print (self):
		print ("Error: " + str (self.code))
		print ("Message: " + self.message)
		if self.type == "DATABASE":
			print ("Query: " + self.query)
		
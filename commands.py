import glob

# Migrate - database migration and creation

# ~~~~~~~~~~~~~~~~~~~~~~ getVersion(dbname) is to look up the version number of the database
def getVersion(dbname):
	[tmp_path,f] = createTempFile('migrate.module/check_version.sql')
	f.write("select %(version)s, %(status)s from patchlevel" %{ 'version':"version", 'status': "status" })
	f.close()
	
	# The format string for running commands through a file
	db_format_string = readConf('migrate.module.file.format')
	command_strings = getCommandStrings()
	command_strings['filename'] = tmp_path
	command_strings['dbname'] = dbname
	db_cmd = db_format_string % command_strings
	
	[code, response] = runDBCommand(db_cmd)
	if code <> 0:
		print "Failure " + response
		sys.exit(-1)

	parts = response.split()
	return [parts[0]," ".join(parts[1:])]
	
# ~~~~~~~~~~~~~~~~~~~~~~ updateVersionTo(dbname,version) updates the version number in the passed database
def updateVersionTo(dbname,version):
	[tmp_path,f] = createTempFile('migrate.module/update_version.sql')
	f.write("update patchlevel set version = %(version)s, status = '%(status)s'" %{ 'version':version, 'status': "Successful" })
	f.close()
	
	# The format string for running commands through a file
	db_format_string = readConf('migrate.module.file.format')
	command_strings = getCommandStrings()
	command_strings['filename'] = tmp_path
	command_strings['dbname'] = dbname
	db_cmd = db_format_string % command_strings
	
	[code, response] = runDBCommand(db_cmd)
	if code <> 0:
		print "~ ERROR updating version number: "
		print "    " + response
		sys.exit(-1)
		
# ~~~~~~~~~~~~~~~~~~~~~~ updateStatusTo(dbname,status) updates the status in the passed database
def updateStatusTo(dbname,status):
	[tmp_path,f] = createTempFile('migrate.module/update_status.sql')
	f.write("update patchlevel set status = '%(status)s'" %{'status': status })
	f.close()
	
	# The format string for running commands through a file
	db_format_string = readConf('migrate.module.file.format')
	command_strings = getCommandStrings()
	command_strings['filename'] = tmp_path
	command_strings['dbname'] = dbname
	db_cmd = db_format_string % command_strings
	
	[code, response] = runDBCommand(db_cmd)
	if code <> 0:
		print "~ ERROR updating status: "
		print "~    " + response
		sys.exit(-1)
		
	
# Constructs a temporary file for use in running SQL commands 
def createTempFile(relative_path):
	tmp_path = os.path.normpath(os.path.join(application_path, 'tmp/' + relative_path))
	pathdir = os.path.dirname(tmp_path)
	if not os.path.exists(pathdir):
		os.makedirs(pathdir)
	return [tmp_path, open(tmp_path,'w')]

	
# ~~~~~~~~~~~~~~~~~~~~~~ getCommandStrings() retrieves the command parameters for running a DB command from the command line
def getCommandStrings():
	db_create_user = readConf('migrate.module.username')
	db_create_pwd = readConf('migrate.module.password')
	db_port = readConf('migrate.module.port')
	db_host = readConf('migrate.module.host')
	return {'username': db_create_user, 'password': db_create_pwd, \
			'host': db_host, 'port': db_port, 'filename': "", 'dbname': "" } 

# ~~~~~~~~~~~~~~~~~~~~~  Runs the specified command, returning the returncode and the text (if any)			
def runDBCommand(command):
	returncode = None
	line = ""
	try:
		create_process = subprocess.Popen(command, env=os.environ, shell=True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
		while True:
			returncode = create_process.poll()
			line += create_process.stdout.readline() 	
			if returncode != None:
				break;
				
	except OSError:
		print "Could not execute the database create script: " 
		print "    " + command
		returncode = -1
	
	return [returncode,line]
	
# ~~~~~~~~~~~~~~~~~~~~~~ Retrieves the list of migration files for the specified database.  The files live in the
# ~~~~~~~~~~~~~~~~~~~~~~ {playapp}/db/migrate/{dbname} folder and follow a naming convention: {number}.{up|down}.{whatever}.sql
def getMigrateFiles(dbname, exclude_before):
	search_path = os.path.join(application_path, 'db/migrate/',dbname + '/*up*.sql')
	
	initial_list = glob.glob(search_path)
	return_obj = {}
	# Filter the list to only the specified pattern
	pat = re.compile('(\d+)\.(up|down).*\.sql\Z')
	maxindex = 0
	for file in initial_list:
		match = re.search(pat,file)
		index = int(match.group(1))
		if match != None and index > exclude_before:
			return_obj[index] = file
		if match != None and index > maxindex:
			maxindex = index
		
	return [maxindex, return_obj]

# ~~~~~~~~~~~~~~~~~~~~~~ [migrate:create] Create the initial database
if play_command == 'migrate:create':
	try:
		# The format string for running commands through a file
		db_format_string = readConf('migrate.module.file.format')
		
		if application_path:
			createpath = os.path.join(application_path, 'db/migrate/create.sql')
			if not os.path.exists(createpath):
				print "~ "
				print "~ Unable to find create script"
				print "~    Please place your database creation script in db/migrate/create.sql"
				print "~ "
				sys.exit(-1)
		else:
			print "~ Unable to find create script"
			sys.exit(-1)		
		
		db_commands = getCommandStrings()
		db_commands['filename'] = createpath
		db_commands['dbname'] = ""

		db_create_cmd = db_format_string %db_commands
			
		print "~ Running create.sql script..."
				
		[code,response] = runDBCommand(db_create_cmd)
		if code <> 0:
			print "~ " + str(code)
			print "~ ERROR: could not execute the database create script: "
			print "~     " + db_create_cmd
			print "~ "
			print "~ Process response: " + response
			print "~ "
			print "~ Check your credentials and your script syntax and try again"
			print "~ "
			sys.exit(-1)
			
	except getopt.GetoptError, err:
		print "~ %s" % str(err)
		print "~ "
		sys.exit(-1)
	
	print "~ "
	print "~ Database creation script completed."
	print "~ "
	sys.exit(0)
	
# ~~~~~~~~~~~~~~~~~~~~~~ [migrate:up] Migrate the database from it's current version to another version
if play_command == 'migrate:up':
	# The format string we'll use to run DB commands
	db_format_string = readConf('migrate.module.file.format')
	
	# Find the databases to iterat
	db_list = readConf('migrate.module.dbs').split(',')
	
	print "~ Database migration:"
	print "~ "
	for db in db_list:
		print "~    Database: %(db)s" % {'db':db}
		[version,status] = getVersion(db)
		print "~    Version: %(version)s" % {'version': version}
		print "~    Status: %(status)s" % {'status': status}
		[maxindex, files_obj] = getMigrateFiles(db,int(version))
		print "~    Max patch version: " + str(maxindex)
		print "~ "
		if maxindex <= int(version):
			print "~    " + db + " is up to date."
			print "~ "
			print "~ "    
			continue
		print "~    Migrating..."
		command_strings = getCommandStrings()
		for i in range(int(version) + 1, maxindex + 1):
			# Skip missed files
			if files_obj[i] == None:
				print "~      Patch " + str(i) + " is missing...skipped"
				continue
				
			command_strings['filename'] = files_obj[i]
			command_strings['dbname'] = db
			db_cmd = db_format_string % command_strings
			
			[code, response] = runDBCommand(db_cmd)
			if code <> 0:
				print "~  Migration failed on patch " + str(i) + "!"
				print "~    ERRROR message: " + response
				updateStatusTo(db,response)
				
				sys.exit(-1)
			print "~      " + str(i) + "..."
		
			updateVersionTo(db,i)
		print "~ "
		print "~    Migration completed successfully"
		print "~ "
		print "~ ------------------------------------"
		print "~ "
		
	sys.exit(0)

# ~~~~~~~~~~~~~~~~~~~~~~ [migrate:version] Output the current version(s) of the datbase(s)
if play_command == 'migrate:version':
	db_list = readConf('migrate.module.dbs').split(',')
	print "~ Database version check:"
	print "~ "
	for db in db_list:
		[version, status] = getVersion(db)
		format = "%(dbname)-20s version %(version)s, status: %(status)s" % {'dbname':db, 'version':version, 'status': status}
		print "~ " + format 
	
	print "~ " 
	sys.exit(0)

# ~~~~~~~~~~~~~~~~~~~~~~ [migrate:init] Build the initial / example files for the migrate module
if play_command == 'migrate:init':
	override('db/migrate/create.sql', 'db/migrate/create.sql')
	override('db/migrate/db1/1.up.create_user.sql', 'db/migrate/db1/1.up.create_user.sql')
	print "~ "
	sys.exit(0)
	
if play_command.startswith('migrate:'):
	print "~ Database migration module "
	print "~  "
	print "~ Use: migrate:create to create your initial database" 
	print "~      migrate:up to migrate your database up" 
	print "~      migrate:version to read the current version of the database" 
	print "~      migrate:init to set up some initial database migration files" 
	print "~ "
	
	sys.exit(0)
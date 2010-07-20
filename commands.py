import glob
import re

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
		print "	" + response
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
		print "~	" + response
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
		print "	" + command
		returncode = -1
	
	return [returncode,line]
	
# ~~~~~~~~~~~~~~~~~~~~~~ Retrieves the list of migration files for the specified database.  The files live in the
# ~~~~~~~~~~~~~~~~~~~~~~ {playapp}/db/migrate/{dbname} folder and follow a naming convention: {number}.{up|down}.{whatever}.sql
def getMigrateFiles(dbname, exclude_before):
	search_path = os.path.join(application_path, 'db/migrate/',dbname + '/*up*.sql')

	initial_list = glob.glob(search_path)
	return_obj = {}
	collisions = []
	# Filter the list to only the specified pattern
	pat = re.compile('(\d+)\.(up|down).*\.sql\Z')
	maxindex = 0
	for file in initial_list:
		match = re.search(pat,file)
		index = int(match.group(1))
		if index in return_obj:
			collisions.append("" + return_obj[index] + "  <==>  " + file)
		if match != None and index > exclude_before:
			return_obj[index] = file
		if match != None and index > maxindex:
			maxindex = index
			
	# Check for collisions
	if len(collisions) > 0:
		print "~"
		print "~ ======================================================================================================"
		print "~ "
		print "~  ERROR:  Migrate collisions detected.  Please resolve these, then try again"
		print "~"
		print "~  Collision list:"
		for item in collisions:
			print "~         " + item
		print "~"
		print "~"
		sys.exit(-1)
		
	# Check for gaps
	missed = []
	for idx in range((exclude_before + 1),maxindex):
		if idx not in return_obj:
			missed.append(idx)
	
	if len(missed) > 0:
		print "~"
		print "~ ======================================================================================================"
		print "~ "
		print "~  ERROR:  Migrate file gaps detected.  Please resolve these, then try again"
		print "~"
		print "~  Files at the following levels are missing:"
		for idx in missed:
			print "~      %s" % idx
		print "~"
		print "~"
		sys.exit(-1)
			
	return [maxindex, return_obj]

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ extracts the database and its alias from the passed database name.
def extractDatabaseAndAlias(db):
	db = db.strip()
		
	# See if there's an alias.
	match = re.search('(\w+)\[(\w+)]',db);
	if match == None:
		db_alias = db;
		db_alias_name = 'None';
	else:
		db_alias = match.group(2);
		db_alias_name = db_alias;
		db = match.group(1);
		
	return [db,db_alias,db_alias_name]
	
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ interpolates the creation file with the passed database name.
def interpolateDBFile(db, createpath):
	[tmp_path,f] = createTempFile('migrate.module/temp_create_%(db)s.sql' % {'db': db})
	print "~ Creating temp file: %(tf)s" % {'tf':tmp_path}
	for line in open(createpath).readlines():
		f.write(line.replace("${db}",db))

	f.close()
	
	return tmp_path
	
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Runs the creation script
def runCreateScript(createpath, createname):
		db_format_string = readConf('migrate.module.file.format')
		db_commands = getCommandStrings()
		db_commands['filename'] = createpath
		db_commands['dbname'] = ""

		db_create_cmd = db_format_string %db_commands
			
		print "~ Running script %(cs)s..." % {'cs': createname}
				
		[code,response] = runDBCommand(db_create_cmd)
		if code <> 0:
			print "~ " + str(code)
			print "~ ERROR: could not execute the database create script: "
			print "~	 " + db_create_cmd
			print "~ "
			print "~ Process response: " + response
			print "~ "
			print "~ Check your credentials and your script syntax and try again"
			print "~ "
			sys.exit(-1)


# ~~~~~~~~~~~~~~~~~~~~~~ Runs the database creation script
def create():
	try:
		is_generic = False
		
		if application_path:
			createpath = os.path.join(application_path, 'db/migrate/generic_create.sql')
			if not os.path.exists(createpath):
				createpath = os.path.join(application_path, 'db/migrate/create.sql')
				if not os.path.exists(createpath):
					print "~ "
					print "~ Unable to find create script"
					print "~	Please place your database creation script in db/migrate/create.sql or db/migrate/generic_create.sql"
					print "~ "
					sys.exit(-1)
			else:
				is_generic = True
		else:
			print "~ Unable to find create script"
			sys.exit(-1)
			
		if is_generic:
			print "~ Using generic create script, replacing parameter ${db} with each database name..."
			print "~ "
			db_list = readConf('migrate.module.dbs').split(',')
			
			for db in db_list:
				# Extract the database name, trimming any whitespace.
				[db, db_alias, db_alias_name] = extractDatabaseAndAlias(db)
				print "~ Database: %(db)s" % {'db': db}
				
				# interpolate the generic file to contain the database name.
				interpolated = interpolateDBFile(db, createpath)
				# run the interpolated creation script
				runCreateScript(interpolated,'generic_created.sql (%(db)s)' % {'db': db})
		else:
			# Run the create script
			runCreateScript(createpath, 'create.sql')
			
	except getopt.GetoptError, err:
		print "~ %s" % str(err)
		print "~ "
		sys.exit(-1)
	
	print "~ "
	print "~ Database creation script(s) completed."
	print "~ "
	
	
# ~~~~~~~~~~~~~~~~~~~~~~ Performs the up migration task
def up():
	# The format string we'll use to run DB commands
	db_format_string = readConf('migrate.module.file.format')
	
	# Find the databases to iterat
	db_list = readConf('migrate.module.dbs').split(',')
	
	print "~ Database migration:"
	print "~ "
	for db in db_list:
		# Extract the database name, trimming any whitespace.
		[db, db_alias, db_alias_name] = extractDatabaseAndAlias(db)
		
		print "~	Database: %(db)s (Alias:%(alias)s)" % {'db':db, 'alias': db_alias_name }
		[version,status] = getVersion(db)
		print "~	Version: %(version)s" % {'version': version}
		print "~	Status: %(status)s" % {'status': status}
		[maxindex, files_obj] = getMigrateFiles(db_alias,int(version))
		print "~	Max patch version: " + str(maxindex)
		print "~ "
		if maxindex <= int(version):
			print "~	" + db + " is up to date."
			print "~ "
			print "~ "	
			continue
		print "~	Migrating..."
		command_strings = getCommandStrings()
		for i in range(int(version) + 1, maxindex + 1):
			# Skip missed files
			if files_obj[i] == None:
				print "~	  Patch " + str(i) + " is missing...skipped"
				continue
				
			command_strings['filename'] = files_obj[i]
			command_strings['dbname'] = db
			db_cmd = db_format_string % command_strings
			
			[code, response] = runDBCommand(db_cmd)
			if code <> 0:
				print "~  Migration failed on patch " + str(i) + "!"
				print "~	ERRROR message: " + response
				updateStatusTo(db,response)
				
				sys.exit(-1)
			print "~	  " + str(i) + "..."
		
			updateVersionTo(db,i)
		print "~ "
		print "~	Migration completed successfully"
		print "~ "
		print "~ ------------------------------------"
		print "~ "
		
# ~~~~~~~~~~~~~~~~~~~~~~ Drops all databases
def dropAll():
	db_list = readConf('migrate.module.dbs').split(',')
	print "~ "
	print "~ Dropping databases..."
	for db in db_list:
		[db, db_alias, db_alias_name] = extractDatabaseAndAlias(db)
		
		print "~	drop %(db)s" % {'db':db}
		[tmp_path,f] = createTempFile('migrate.module/drop_db.sql')
		f.write("drop database if exists %(db)s;" %{ 'db':db })
		f.close()
		
		# The format string for running commands through a file
		db_format_string = readConf('migrate.module.file.format')
		command_strings = getCommandStrings()
		command_strings['filename'] = tmp_path
		command_strings['dbname'] = ""
		db_cmd = db_format_string % command_strings
		
		[code, response] = runDBCommand(db_cmd)
		if code <> 0:
			print "Failure " + response
			sys.exit(-1)
	print "~ "
	print "~ Database drop completed"
	print "~ "

# ~~~~~~~~~~~~~~~~~~~~~~ [migrate:create] Create the initial database
if play_command == 'migrate:create':
	create()
	sys.exit(0)
	
# ~~~~~~~~~~~~~~~~~~~~~~ [migrate:up] Migrate the database from it's current version to another version
if play_command == 'migrate:up':
	up()
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

# ~~~~~~~~~~~~~~~~~~~~~~ [migrate:init] Build the initial / example files for the migrate module
if play_command == 'migrate:drop-rebuild':
	dropAll()
	create()
	up()
	sys.exit(0)
	
if play_command.startswith('migrate:'):
	print "~ Database migration module "
	print "~  "
	print "~ Use: migrate:create to create your initial database" 
	print "~	  migrate:up to migrate your database up" 
	print "~	  migrate:version to read the current version of the database" 
	print "~	  migrate:init to set up some initial database migration files" 
	print "~	  migrate:drop-rebuild to drop and then rebuild all databases (use with caution!)"
	print "~ "
	
	sys.exit(0)
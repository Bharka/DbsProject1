import vertica_python
import sys

# file login.ini contains host, username, password, and db name
#with open('login.ini', 'r') as f:
#	host = f.readline().strip()
#	username = f.readline().strip()
#	password = f.readline().strip()
#	database = f.readline().strip()

# EStablishes the connection 
class ColumnError(Exception):
	def __init__(self, value):
		self.value = value
	def __str__(self):
		return repr(self.value)

def connection():
	conn_info = {'host': '127.0.0.1',
			 'port': 5433,
			 'user': 'dba_bharka',
			 'password':'',
			 'database': 'vertica',
			 'read_timeout': 600,
			 'connection_timeout': 5}
	try:
		connection = vertica_python.connect(**conn_info)
		#cur = connection.cursor()
		return connection
	except:
		print("there has been an connection error")
############# Create Output Table#############
def output(cursor):
	queryCreateTable="create table NFtxt(tabName varchar2(20),threeNF varchar2(20),Failed varchar2(20),Reason char(200));"
	try:
		query="select * from NFtxt;"
		#print(query)
		cursor.execute(query)
		cursor.fetchall()
		queryCreate="drop table NFtxt;"
		cursor.execute(queryCreate)
		try:
	 		cursor.execute(queryCreate)
	 	except(vertica_python.errors.VerticaSyntaxError):
	 		print("Syntax error in creating table  ")

	except(vertica_python.errors.MissingRelation):
	 	print("Table Not found, we are creating table ")
	 	try:
	 		cursor.execute(queryCreateTable)
	 	except(vertica_python.errors.VerticaSyntaxError):
	 		print("Syntax error in creating table  ")

	descTable= "select * from Nftxt;"
	print cursor.execute(descTable)


########### out put table ends #########

########### check if each table exists in DB############
def checkTable(tableName,dictTablePrimaryKeys,dictTableNonPrimeAttributes,cursor):
	try:
		query="select * from "+tableName+" ;"
		#print(query)
		val=cursor.execute(query)
		valuesFromTextFile=dictTablePrimaryKeys.get(tableName)+dictTableNonPrimeAttributes.get(tableName)
		print("these are values from text",valuesFromTextFile)
		arrayInDb=[]
		for each in cursor.description:
			arrayInDb.append(each[0])
		print(arrayInDb)
		for each in valuesFromTextFile:
			if(each not in arrayInDb):
				print("this value not in Db ",each)
				raise ColumnError(each)
	except(vertica_python.errors.MissingRelation):
		print(tableName,"Does not exist")
	 	return False
	except ColumnError as e:
		print("Column "+e.value +" Does not exist in table "+tableName)
		return False
	return True
################# check if Table exists ends ########

#################### function check null values START ####ame############
def checkNullVlaues(table_name,dictTablePrimaryKeys,cur):
	dictCheckNullValues={}
	#print("tables ", table_name,"Dictionary keys ",dictTablePrimaryKeys)
	keysString=""
	primaryKeysForEachTable=dictTablePrimaryKeys.get(table_name)
	keyIndex=0;
	keysString=""
	while(keyIndex<len(primaryKeysForEachTable)):
		keysString+=str(primaryKeysForEachTable[keyIndex])
		keysString+=" is NULL "
		if(keyIndex!=len(primaryKeysForEachTable)-1):
			keysString+=" OR "
		keyIndex+=1
	#print keysString
	query ="select * \n from " + table_name +" \n where "+keysString
	f.write(query+"\n\n")
	try:
		cur.execute(query)
		#print 'query in try:'+query
		cur.fetchall()
		#print 'rowcount is:',cur.rowcount
		dictCheckNullValues[table_name]=cur.rowcount
	except:
		print("there has been an exception")
	return dictCheckNullValues
#################### function check null values END ################

################## function Check Duplicate Values #################
def checkDuplicates(table_name,dictTablePrimaryKeys,cur):
	dictCheckDuplicates={}
	#print("tables ", table_name,"Dictionary keys ",dictTablePrimaryKeys)
	keysString=""
	primaryKeysForEachTable=dictTablePrimaryKeys.get(table_name)
	keyIndex=0;
	keysString=""
	while(keyIndex<len(primaryKeysForEachTable)):
		keysString+=str(primaryKeysForEachTable[keyIndex])
		if(keyIndex!=len(primaryKeysForEachTable)-1):
			keysString+=","
		keyIndex+=1
	#print keysString
	query ="select "+ keysString +",count(*) \n from " + table_name +" \n group by "+keysString+" \n having count(*) > 1;"
	f.write(query+"\n \n")
	try:
		cur.execute(query)
		cur.fetchall()
		dictCheckDuplicates[table_name]=cur.rowcount
	except:
		print("there has been an exception")
	return dictCheckDuplicates

################## function Check Duplicate Values END #################
################### 		1NF START 				#################
def check1Nf(table_name,dictTablePrimaryKeys,dictTableNonPrimeAttributes,cur):
	#f.write("NULL VALUES ")
	nullValues =checkNullVlaues(table_name,dictTablePrimaryKeys,cur)
	#f.write("DUPLICATES ")
	duplicateValues =checkDuplicates(table_name,dictTablePrimaryKeys,cur)
	if(nullValues.get(table_name)!=-1 or duplicateValues.get(table_name)!=-1):
		if(nullValues.get(table_name)!=-1):
			return "NUll Values Exist"
		else:
			return "Duplicate Values Exist"
	else:
		return True
		

##################### 		1 NF FAILED 		##################

####################	printing which Query Failed ############
def output2NF(primaryString,nonKeyString):
	#print(primaryString,nonKeyString)
	primaryKeyArray=primaryString.split(" = " )
	key=primaryKeyArray[0].split(".")
	#print("key is ",key[1])
	nonPrimaryKeyArray=nonKeyString.split(" <> " )
	nonPrimekey=nonPrimaryKeyArray[0].split(".")
	outputString=str(key[1])+"->"+str(nonPrimekey[1])+" , "
	return(outputString)


####################### 	Functions for 2NF ##################
def checkFunctionalDependecy(table,dictTablePrimaryKey,dictTableNonPrimeAttributes,cursor):
	#print("these are the tables with more than 1 primary keys",functionalDependecyTables)
	outputString=""
	fullyFunctionalDependentArray=[]
	#for each in functionalDependecyTables:
	functionalDependecyCount=0
	query=""
	query+="select * \n from "+table+" p1, "+table +" p2 \n where "
	index=0
	tempPrimaryKeyTable=dictTablePrimaryKey.get(table)
	while(index<len(tempPrimaryKeyTable)):
		tempPrimaryString=""
		tempPrimaryString+="p1."+tempPrimaryKeyTable[index]
		tempPrimaryString+=" = "
		tempPrimaryString+="p2."+tempPrimaryKeyTable[index] +" and "
		index+=1
		indexNonPrimary=0
		tempNonPrimaryKey=dictTableNonPrimeAttributes.get(table)
		while(indexNonPrimary<len(tempNonPrimaryKey)):
			tempNonPrimeString=""
			tempNonPrimeString+="p1."+tempNonPrimaryKey[indexNonPrimary]
			tempNonPrimeString+=" <> p2."+tempNonPrimaryKey[indexNonPrimary]
			#print query+tempPrimaryString+tempNonPrimeString
			finalStatement=query+tempPrimaryString+tempNonPrimeString
			f.write(finalStatement+"\n \n")
			cursor.execute(finalStatement)
			cursor.fetchall()
			if(cursor.rowcount!=-1):
				functionalDependecyCount+=1
			else:
				#print("final statement is ",finalStatement)
				#print("functional Depedency failed for table",table)
				outputString+=output2NF(tempPrimaryString,tempNonPrimeString)+" "
			indexNonPrimary+=1
	#print(functionalDependecyCount,len(dictTablePrimaryKey.get(each)), len(dictTableNonPrimeAttributes.get(each)))
	if(functionalDependecyCount==len(dictTablePrimaryKey.get(table))* len(dictTableNonPrimeAttributes.get(table))):
		return True
	else:
		#print("functional dependency failed for",table)
		return outputString
####################### 	Functions for 2NF END ##################

####################### CHECK 2NF ##################################
def check2Nf(table,dictTablePrimaryKey,dictTableNonPrimeAttributes,cursor):
	threeNFArray=[]
	functionalDependecyTables=[]
	lengthOfPrimarykeys=0
	lengthOfPrimarykeys=len(dictTablePrimaryKey.get(table))
	if(lengthOfPrimarykeys==1):
		#print("this table has 1 key and  so its fuly functional dependent",table)
		return True
	else:
		outputString=checkFunctionalDependecy(table,dictTablePrimaryKey,dictTableNonPrimeAttributes,cursor)
		if(outputString==True):
			#print("this is fully functionally depedent",table)
			return True
		else:
			return outputString

####################### CHECK 2NF END ##################################
######################## function for functional depenecy for non prime attributes ##############
def functionalDependencyNonPrime(table,dictTableNonPrimeAttributes,cursor):
	outputString=""
	bcnfArray=[]
	tempNonPrimeAttributes=[]
	counterOfNonPrime=0
	tempNonPrimeAttributes=dictTableNonPrimeAttributes.get(table)
	string ="select * \n from "+table+" np1 , "+table+" np2 \n where np1."
	index=0
	nonPrimeAttributes=dictTableNonPrimeAttributes.get(table)
	#print("length of non prime attributes ",len(nonPrimeAttributes))
	while(index < len(nonPrimeAttributes)):
		refNonPrime=""
		refNonPrime+=nonPrimeAttributes[index]+" = np2."+nonPrimeAttributes[index]+" and "
		index_inner=0
		subArrayNonPrimeAttributes=dictTableNonPrimeAttributes.get(table)
		#print("length of non prime before removing ",len(nonPrimeAttributes),"length after removing ",len(subArrayNonPrimeAttributes))
		while(index_inner<len(subArrayNonPrimeAttributes)):
			if(subArrayNonPrimeAttributes[index_inner]!=nonPrimeAttributes[index]):
				otherNonPrimeString=""
				otherNonPrimeString+=" np1."+subArrayNonPrimeAttributes[index_inner]+" <> np2."+ subArrayNonPrimeAttributes[index_inner]+";"
				#print("this is the fucking string ",string+refNonPrime+otherNonPrimeString)
				query=string+refNonPrime+otherNonPrimeString
				#print("query is ",query)
				f.write(query+"\n \n")
				cursor.execute(query)
				cursor.fetchall()
				if(cursor.rowcount==-1):
					#print("functional dependecy exists for ",nonPrimeAttributes[index]+"->"+subArrayNonPrimeAttributes[index_inner])
					outputString+=nonPrimeAttributes[index]+"->"+subArrayNonPrimeAttributes[index_inner]+" , "
				else:
					#print counterOfNonPrime
					counterOfNonPrime+=1
			index_inner+=1
		index+=1
	if(counterOfNonPrime==len(tempNonPrimeAttributes)*(len(tempNonPrimeAttributes)-1)):
		print("3NF holds for ",table)
		return True
	else:
		#print("3NF does not hold for ",table)
		#print ("the output string is ",outputString)
		return outputString
######################## function for functional depenecy for non prime attributes ##############


def check3Nf(table,dictTablePrimaryKey,dictTableNonPrimeAttributes,cursor):
	#bcnfArray=[]
	index=0
	tableWithMoreThanOneNonPrime=[]
	if(len(dictTableNonPrimeAttributes.get(table))==1):
		#print("3Nf holds for as it has 1 non primary key ",table)
		return True
	else:
		outputString=functionalDependencyNonPrime(table,dictTableNonPrimeAttributes,cursor)
		if(outputString==True):
			#checkBcnf(table,dictTablePrimaryKey,dictTableNonPrimeAttributes,cursor)
			return True
		else:
			return outputString

def checkBcnf(bcnfTable,dictTablePrimaryKey,dictTableNonPrimeAttributes,cursor):
	#f.write("BCNF Queries ")
	outputString=checkFunctionalDependecy(bcnfTable,dictTableNonPrimeAttributes,dictTablePrimaryKey,cursor)
	if(outputString==True):
		#print ("the bcnf success for",bcnfTable)
		return True
	else:
		#print ("the bcnf Falied for",bcnfTable)
		return outputString
	#f.close()


# File open and save 
with open(sys.argv[1], 'r') as my_file:
	row=my_file.readlines()
	table_name =list()
	dictTablePrimaryKeys={}
	dictTableNonPrimeAttributes={}
	for each in row:
		nonPrimeAttributes=[]
		primary_key1=[]
		tableStartName=each.split("(")
		#print("each is ",tableStartName[0])
		table_name.append(tableStartName[0])
		if('\n' in each ):
			primeNonPrime=(each[len(tableStartName[0])+1:len(each)-2]).split(",")
		else:
			primeNonPrime=(each[len(tableStartName[0])+1:len(each)-1]).split(",")
		for value in primeNonPrime:
			if('(k)' in value):
				temp=value.replace("(k)","")
				#print("primary Key is ",temp)
				primary_key1.append(temp)
			else:
				nonPrimeAttributes.append(value)
		dictTablePrimaryKeys[tableStartName[0]]=primary_key1
		dictTableNonPrimeAttributes[tableStartName[0]]=nonPrimeAttributes
	index=0
	tableWeAreSendingToNF=[]
	#print(table_name)
	try:
		#open('queriesFile.txt', 'w').close()
		with open('queriesFile.txt', 'w') as f:
			data=f.read()
			#print("data is ",data)
			
	except(IOError):
		f = open('queriesFile.txt','w+') 
	
	connection=connection()
	cursor=connection.cursor()
	if(cursor):
		while(index<len(table_name)):
		#print ("table name is ",table_name[index] ," primary keys are ",dictTablePrimaryKeys.get(table_name[index]),"Non prime attribuetes are ",dictTableNonPrimeAttributes.get(table_name[index]))
			if(checkTable(table_name[index],dictTablePrimaryKeys,dictTableNonPrimeAttributes,cursor) == True):
				tableWeAreSendingToNF.append(table_name[index])
			index+=1
		output(cursor)
		print("Final tables, remove tables which are not there  ",tableWeAreSendingToNF)
		for eachTable in tableWeAreSendingToNF:
			f.write("Table "+eachTable+"\n")
			oneNfResult=check1Nf(eachTable,dictTablePrimaryKeys,dictTableNonPrimeAttributes,cursor)
			if(oneNfResult==True):
				twoNfResult=check2Nf(eachTable,dictTablePrimaryKeys,dictTableNonPrimeAttributes,cursor)
				#print("this is the 2nf result ",twoNfResult)
				if(twoNfResult==True):
					threeNfResult=check3Nf(eachTable,dictTablePrimaryKeys,dictTableNonPrimeAttributes,cursor)
					if(threeNfResult==True):
						bcnfResult=checkBcnf(eachTable,dictTablePrimaryKeys,dictTableNonPrimeAttributes,cursor)
						#print("bcnf r")
						if(bcnfResult==True):
							print("BCNF passed",eachTable)
							#NFtxt(tabName varchar2(20),threeNF varchar2(20),Failed varchar2(20),Reason char(200));
							query="Insert into NFtxt values('"+eachTable+"', 'YES' ,'NA', 'NA' );"
							cursor.execute(query)
							connection.commit()
						else:
							print("Bcnf failed",eachTable)
							query="Insert into NFtxt values('"+eachTable+"', 'NO' ,'BCNF ','"+ bcnfResult+"' );"
							cursor.execute(query)
							connection.commit()
							print("the out put for bcnf is",bcnfResult)
					else:
						print("3Nf failed for ",eachTable)
						print("output for 3 nf is ",threeNfResult)
						query="Insert into NFtxt values('"+eachTable+"', 'NO' ,'3NF','"+threeNfResult +"' );"
						cursor.execute(query)
						connection.commit()
				else:
					print("2Nf failed",eachTable)
					print("2nf result is ",twoNfResult)
					query="Insert into NFtxt values('"+eachTable+"', 'NO' ,'2NF','"+twoNfResult +"' );"
					cursor.execute(query)
					connection.commit()
			else:
				print("oneNf Failed",eachTable)
				query="Insert into NFtxt values('"+eachTable+"', 'NO' ,'1NF','"+oneNfResult+"');"
				cursor.execute(query)
				connection.commit()
		f.close()
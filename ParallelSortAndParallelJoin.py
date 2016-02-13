#!/usr/bin/python3.4
#
# Interface for the assignement
#

import psycopg2
import os
import csv
import sys
import fileinput
import time
import threading
#ratingstablename = 'Ratings'
Table_1='Table_1'



#Connection function to establish conection with the database created
def getopenconnection(user='postgres', password='Tanushree', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

#Creates a file with comma seperated values instead of '::' as delimiter
def loadratings(ratingsfilepath, openconnection):
    inputFile=open(ratingsfilepath)
    outputFile=open("ratings1.dat","w")
    changeDelimiter={'::':','}
    with open('ratings1.dat', 'w') as f:
        for line in inputFile:
            for inputFile, outputFile in changeDelimiter.iteritems():
                line=line.replace(inputFile, outputFile)
                f.write(line)
    createtable(openconnection)

#This function is called from loadratings itself and it loads the csv files in the database    
def createtable(con):
    file1=open('ratings1.dat','rb')
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS Ratings CASCADE")
    cur.execute("CREATE TABLE Ratings(UserID integer, MovieID integer, Rating float4, Timestamp bigint)")
    #print("DONE!")
    con.commit()
    cur.copy_from(file1,'Ratings',sep=",")
    cur.execute("ALTER TABLE Ratings DROP COLUMN TimeStamp")
    con.commit()
	cur.execute("CREATE TABLE TEASE(RATE INTEGER)")
	con.commit()

#This partition creates ranges according to the number of partitions given as input and inserts the values of the table according to the rating attribute   
def rangepartition(ratingstablename,SortingColumnName,maxval,minval,numberofpartitions, con):
	cur=con.cursor()
	start=minval-1
	#print str(start)
	#print str(maxval)
	division=float(maxval/5)
	end=division
    #division=5/numberofpartitions
	i=1
	#dbname='dds_assgn1'
	#user='postgres'
	#password='Tanushree'
    #con = psycopg2.connect("dbname='" + dbname + "' user='" +user + "' host='localhost' password='" + password + "'")
	
	while i<=numberofpartitions:
        #print "Value of i: "+str(i)
		cur.execute("DROP TABLE IF EXISTS Table_"+str(i)+" CASCADE")
		con.commit()
		cur.execute("SELECT * INTO Table_"+str(i)+" FROM "+ratingstablename+" WHERE "+SortingColumnName+">"+str(start)+" AND "+SortingColumnName+"<="+str(end))
		con.commit()
		start=end
		end=end+division
		i=i+1
	cur.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)", ('rangedata',))
	count = cur.fetchone()[0]
	con.commit()



#This function partitions the existing table according to round robin fashion    
def roundrobinpartition(ratingstablename, numberofpartitions, con):
    startTime = time.time()
    count=0
    i=1
    j=1
    k=1
    remaining=0
    #con = psycopg2.connect("dbname='" + dbname + "' user='" +user + "' host='localhost' password='" + password + "'")
    cur = con.cursor()
    cur.execute("SELECT * FROM "+str(ratingstablename))
    rows=cur.fetchall()
    
    for row in rows:
        count=count+1
    oset=count/numberofpartitions
    q_oset=0

    while i<=numberofpartitions:
        cur.execute("DROP TABLE IF EXISTS RR_"+str(i))
        cur.execute("CREATE TABLE RR_"+str(i)+"(UserID integer, MovieID integer, Rating float4)")
        con.commit()
        while j<=oset:
            cur.execute("SELECT * FROM "+str(ratingstablename)+" LIMIT 1 OFFSET "+str(q_oset))
            current_records=cur.fetchall()
            cur.execute("INSERT INTO RR_"+str(i)+"(UserID,MovieID,Rating) VALUES("+str(current_records[0][0])+","+str(current_records[0][1])+","+str(current_records[0][2])+")")
            q_oset=q_oset+numberofpartitions
            j=j+1
            con.commit()
        j=1
        q_oset=i
        i=i+1
		
	remaining = count-(numberofpartitions*oset)
	if remaining>0:
            new_oset = count-remaining
            new_qoset=0
        while k<=remaining:
            cur.execute("SELECT * FROM "+ratingstablename+" LIMIT 1 OFFSET "+str(new_qoset))
            current_records_new=cur.fetchall()
            cur.execute("INSERT INTO RR_"+str(k)+"(UserID, MovieID, Rating) VALUES("+str(current_records_new[0][0])+","+str(current_records_new[0][1])+","+str(current_records_new[0][2])+")")
            con.commit()
            new_qoset=new_qoset+1
            k=k+1
        cur.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)", ('roundrobindata',))
    count = cur.fetchone()[0]
    if count:
        print "Done"
        #print count
    else:
        cur.execute("CREATE TABLE roundrobindata(met INTEGER, lastentry INTEGER)")
        con.commit()
    if(remaining==0):
        cur.execute("INSERT INTO roundrobindata(met,lastentry) VALUES ("+str(numberofpartitions)+","+str(numberofpartitions)+")")
        con.commit()
    else:
        cur.execute("INSERT INTO roundrobindata(met,lastentry) VALUES ("+str(numberofpartitions)+","+str(remaining)+")")
        con.commit()

    endTime = time.time()
    #print "Total time: "+str((endTime-startTime)/60)

#This function inserts a value entered according to round robin
def roundrobininsert(ratingstablename, userid, itemid, rating, con):
    cur=con.cursor()
    cur.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)", ('roundrobindata',))
    count = cur.fetchone()[0]
    cur.execute("Select * from roundrobindata")
    value=cur.fetchall()
    lastentry=0
    for v in value:
        total = v[0]
        N=v[1]
    if count:
        #division=5/float(N)
        #tno=(rating/division)+1
        if total==N:
            cur.execute("INSERT INTO rr_1(UserID,MovieID,Rating) VALUES("+str(userid)+","+str(itemid)+","+str(rating)+")")
            con.commit()
            lastentry=1
        else:
            cur.execute("INSERT INTO rr_"+str(int(N+1))+"(UserID,MovieID,Rating) VALUES("+str(userid)+","+str(itemid)+","+str(rating)+")")
            con.commit()
            lastentry=N+1
        
        #print "TNO: "+str(int(tno))
        cur.execute("INSERT INTO roundrobindata(met,lastentry) VALUES ("+str(total)+","+str(lastentry)+")")
        con.commit()
    else:
        print("No roundrobin partitions available")

#This function inserts a value entered according to range in which the rating fits    
def rangeinsert(ratingstablename, userid, itemid, rating, con):
    cur=con.cursor()
    cur.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)", ('rangedata',))
    count = cur.fetchone()[0]
    cur.execute("Select * from rangedata")
    value=cur.fetchall()
    for v in value:
        N=v[0]
    if count:
        division=5/float(N)
        if(rating==0):
            tno=1
        elif((rating/division)%1 == 0):
            tno=(rating/division)
        else:
            tno=(rating/division)+1
        cur.execute("INSERT INTO Table_"+str(int(tno))+"(UserID,MovieID,Rating) VALUES("+str(userid)+","+str(itemid)+","+str(rating)+")")
        con.commit()
        #print "TNO: "+str(int(tno))
    else:
        print("No range partitions available")

#This function eastablishes the connection by itself and deletes all previously created round robin and range partitions along with metadata tables
def DeletePartitions():
    con=getopenconnection()
    cur=con.cursor()
    i=1
    cur.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)", ('rangedata',))
    countr = cur.fetchone()[0]
    if countr:
        cur.execute("SELECT MAX(met) FROM rangedata")
        maxval=cur.fetchone()[0]
        con.commit()
        #print str(maxval)
        while i<=maxval:
            cur.execute("DROP TABLE Table_"+str(i))
            i=i+1
        print("Range tables deleted")
        cur.execute("DROP TABLE rangedata")
        con.commit()

    else:
        print "No Range Partition tables were found."

    
    cur.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)", ('roundrobindata',))
    countrr = cur.fetchone()[0]
    if countrr:
        j=1
        cur.execute("SELECT MAX(met) FROM roundrobindata")
        maxval1=cur.fetchone()[0]
        con.commit()
        #print str(maxval1)
        while j<=maxval1:
            cur.execute("DROP TABLE rr_"+str(j))
            j=j+1
        print("Roundrobin tables deleted")
        cur.execute("DROP TABLE roundrobindata")
        con.commit()

    else:
        print "No roundrobin tables were found."
    

	
def ParallelSort(Table,SortingColumnName,OutputTable,openconnection):
	cur=openconnection.cursor()
	cur.execute("SELECT MAX("+SortingColumnName+") from "+Table)
	maxval=cur.fetchone()[0]
	openconnection.commit()
	cur.execute("SELECT MIN("+SortingColumnName+") from "+Table)
	minval=cur.fetchone()[0]
	openconnection.commit()
	rangepartition(Table,SortingColumnName,maxval,minval,5,openconnection)
	cur.execute("drop table if exists "+str(OutputTable))
	cur.execute("CREATE TABLE "+ str(OutputTable)+ " (LIKE "+ Table +")")
	openconnection.commit()
	cur.execute("ALTER TABLE "+ str(OutputTable)+ " ADD TupleOrder INT")
	openconnection.commit()
	cur.execute("SELECT COUNT("+str(SortingColumnName)+") FROM Table_1")
	count_1=cur.fetchone()[0]
	cur.execute("SELECT COUNT("+str(SortingColumnName)+") FROM Table_2")
	count_2=cur.fetchone()[0]
	cur.execute("SELECT COUNT("+str(SortingColumnName)+") FROM Table_3")
	count_3=cur.fetchone()[0]
	cur.execute("SELECT COUNT("+str(SortingColumnName)+") FROM Table_4")
	count_4=cur.fetchone()[0]
	openconnection.commit()
	#print ("range done")
	t1=threading.Thread(Sort('Table_1',SortingColumnName,OutputTable,1,openconnection))
	t2=threading.Thread(Sort('Table_2',SortingColumnName,OutputTable,(count_1+1),openconnection))
	t3=threading.Thread(Sort('Table_3',SortingColumnName,OutputTable,((count_1+count_2)+1),openconnection))
	t4=threading.Thread(Sort('Table_4',SortingColumnName,OutputTable,((count_1+count_2+count_3)+1),openconnection))
	t5=threading.Thread(Sort('Table_5',SortingColumnName,OutputTable,((count_1+count_2+count_3+count_4)+1),openconnection))
	t1.start()
	t2.start()
	t3.start()
	t4.start()
	t5.start()
	t1.join()
	t2.join()
	t3.join()
	t4.join()
	t5.join()
	#print str(count_1+count_2)
	
	
	
	
def Sort(Table,SortingColumnName,OutputTable,startIndex,con):
	cur=con.cursor()
	cur.execute("DROP INDEX if exists index_name")
	cur.execute("CREATE INDEX index_name ON "+str(Table)+"("+str(SortingColumnName)+")")
	con.commit()
	cur.execute("CLUSTER "+str(Table)+" USING index_name")
	con.commit()
	cur.execute("ALTER TABLE "+ str(Table) + " ADD TupleOrder INT")
	con.commit()
	cur.execute("DROP SEQUENCE IF EXISTS T_SEQ")
	cur.execute("CREATE SEQUENCE T_SEQ START "+str(startIndex)+" INCREMENT 1")
	con.commit()
	cur.execute("UPDATE "+str(Table)+ " SET TupleOrder=nextval('T_SEQ')" )
	con.commit()
	cur.execute("INSERT INTO "+str(OutputTable)+" SELECT * FROM "+str(Table))
	con.commit()
	
	
def Merge(InputTable2,PartitionTable, Table1JoinColumn, Table2JoinColumn,OutputTable,con):
	cur=con.cursor()
	#print "Entering"
	cur.execute("INSERT INTO "+str(OutputTable)+" (SELECT * FROM "+str(PartitionTable)+", "+str(InputTable2) +" WHERE "+str(InputTable2)+"."+str(Table1JoinColumn)+ " = "+str(PartitionTable)+"."+str(Table2JoinColumn)+")")
	#print "Done"
	#rows=cur.fetchall()
	#for r in rows:
	#a=r[0]
	#print str(a)
	#cur.execute("INSERT INTO "+str(OutputTable)+" SELECT * FROM "+str())
	con.commit()
	#print "Updated"
	con.commit()
	
	
	
def ParallelJoin(InputTable1,InputTable2,Table1JoinColumn,Table2JoinColumn,OutputTable,openconnection):
	cur=openconnection.cursor()
	cur.execute("DROP TABLE IF EXISTS "+str(OutputTable))
	cur.execute("CREATE TABLE "+str(OutputTable)+" AS SELECT * FROM "+str(InputTable2)+" , "+str(InputTable1)+" WHERE 1=0 ")
	openconnection.commit()
	cur=openconnection.cursor()
	cur.execute("SELECT MAX("+Table2JoinColumn+") from "+InputTable2)
	maxval=cur.fetchone()[0]
	openconnection.commit()
	cur.execute("SELECT MIN("+Table2JoinColumn+") from "+InputTable2)
	minval=cur.fetchone()[0]
	openconnection.commit()
	rangepartition(InputTable2,Table2JoinColumn,maxval,minval,5,openconnection)
	print str(minval)
	print str(maxval)
	t11=threading.Thread(Merge(InputTable1,'Table_1',Table1JoinColumn,Table2JoinColumn,OutputTable,openconnection))
	t21=threading.Thread(Merge(InputTable1,'Table_2',Table1JoinColumn,Table2JoinColumn,OutputTable,openconnection))
	t31=threading.Thread(Merge(InputTable1,'Table_3',Table1JoinColumn,Table2JoinColumn,OutputTable,openconnection))
	t41=threading.Thread(Merge(InputTable1,'Table_4',Table1JoinColumn,Table2JoinColumn,OutputTable,openconnection))
	t51=threading.Thread(Merge(InputTable1,'Table_5',Table1JoinColumn,Table2JoinColumn,OutputTable,openconnection))
	t11.start()
	t21.start()
	t31.start()
	t41.start()
	t51.start()
	t11.join()
	t21.join()
	t31.join()
	t41.join()
	t51.join()
	#print "All done"



    
def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = psycopg2.connect(user='postgres', host='localhost', password='Tanushree')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database

if __name__ == '__main__':
    try:
        create_db('dds_assgn1')

        with getopenconnection() as con:
            loadratings('ratings.dat', con)
            
    except Exception as detail:
        print "OOPS! This is the error ==> ", detail

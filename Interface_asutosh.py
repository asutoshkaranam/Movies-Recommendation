#!/usr/bin/python2.7
#
# Interface for the assignement
#

# Asutosh Karanam - ASU ID: 1227953352

import psycopg2

def getOpenConnection(user='postgres', password='280472', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):

    cursor = openconnection.cursor()
    #Drop the pre-existing public tables
    dropCmd = "DROP TABLE IF EXISTS " + ratingstablename + ";"
    cursor.execute(dropCmd);

    createCmd = "CREATE TABLE " + ratingstablename + "(UserID INT not null, MovieID INT, Rating FLOAT);"
    cursor.execute(createCmd);

    dataFile = open(ratingsfilepath, 'r')
    rowCnt = dataFile.readlines()
    for row in rowCnt:
        tokens = row.split("::")
        u_id = str(tokens[0])
        m_id = str(tokens[1])
        rating = str(tokens[2])
        insertCmd = "INSERT INTO " + ratingstablename + "(UserID, MovieID, Rating) VALUES ({}, {}, {});".format(u_id, m_id, rating)
        cursor.execute(insertCmd)

    dataFile.close()
    cursor.close()


def rangePartition(ratingstablename, numberofpartitions, openconnection):

    cursor = openconnection.cursor()

    #define the partition length
    prtn_length = 5.0 / numberofpartitions
    prtn_begin = "CREATE TABLE range_part{} AS SELECT UserID, MovieID, Rating FROM " + ratingstablename + " WHERE Rating>={} and Rating<={};"
    prtn_augment = "CREATE TABLE range_part{} AS SELECT UserID, MovieID, Rating FROM " + ratingstablename + " WHERE Rating>{} and Rating<={};"

    for i in range(0, numberofpartitions):
        if (i == 0):
            cursor.execute(prtn_begin.format(i, i*prtn_length, (i+1)*prtn_length))
        else:
            cursor.execute(prtn_augment.format(i, i*prtn_length, (i+1)*prtn_length))

    cursor.close()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):

    cursor = openconnection.cursor()

    for i in range(0, numberofpartitions):

        dropCmd = "DROP TABLE IF EXISTS rrobin_part{};".format(str(i))
        cursor.execute(dropCmd)

        createCmd = "CREATE TABLE rrobin_part{} (UserID INT, MovieID INT, Rating FLOAT);".format(str(i));
        cursor.execute(createCmd)

    cursor.execute( "SELECT * FROM {};".format(ratingstablename))

    rowCnt = cursor.fetchall()

    prtn_num = 0

    for row in rowCnt:
        insertCmd = "INSERT INTO rrobin_part{}".format(prtn_num) + " VALUES(" + str(row[0]) + "," + str(row[1]) + "," + str(row[2]) + ");"
        cursor.execute(insertCmd)
        prtn_num = prtn_num + 1
        prtn_num = prtn_num % numberofpartitions

    openconnection.commit()
    cursor.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):

    cursor = openconnection.cursor()

    insertCmd = "INSERT INTO " + ratingstablename + " VALUES(" + str(userid) + "," + str(itemid) + "," + str(rating) + ");"
    cursor.execute(insertCmd)

    #get the count of total number of partitions
    cursor.execute( "SELECT * FROM information_schema.tables WHERE table_schema = 'public' and table_name like 'rrobin_part%';")
    prtn_cnt = len(cursor.fetchall())

    cursor.execute("SELECT * FROM {};".format(ratingstablename))
    rowCnt = len(cursor.fetchall())

    location = (rowCnt - 1) % prtn_cnt

    cursor.execute("INSERT INTO rrobin_part{} VALUES ({},{},{});".format(location, userid, itemid, rating))
    cursor.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):

    cursor = openconnection.cursor()

    insertCmd = "INSERT INTO " + ratingstablename + " VALUES(" + str(userid) + "," + str(itemid) + "," + str(rating) + ");"
    cursor.execute(insertCmd)

    #get the count of total number of partitions
    cursor.execute( "SELECT * FROM information_schema.tables WHERE table_schema = 'public' and table_name like 'range_part%';")
    prtn_cnt = len(cursor.fetchall())

    prtn_length = 5.0/prtn_cnt

    insertPrtnCmd = "INSERT INTO range_part{} VALUES ({},{},{})"

    for i in range(prtn_cnt):
        if i==0:
            if rating>=i*prtn_length and rating<=(i+1)*prtn_length:
                cursor.execute(insertPrtnCmd.format(i, userid, itemid, rating))
        else:
            if rating>i*prtn_length and rating<=(i+1)*prtn_length:
                cursor.execute(insertPrtnCmd.format(i, userid, itemid, rating))

    cursor.close()

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()

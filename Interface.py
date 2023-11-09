#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):

    output_arr = []

    cursor = openconnection.cursor()

    rangeCmd = 'SELECT * FROM rangeratingspart{0} WHERE rating>={1} and rating<={2};'
    countCmd = 'SELECT partitionnum FROM roundrobinratingsmetadata;'

    execCmd = 'SELECT partitionnum FROM rangeratingsmetadata WHERE maxrating>={0} AND minrating<={1};'.format(ratingMinValue, ratingMaxValue)
    cursor.execute(execCmd)

    prtn_arr = [prtn[0] for prtn in cursor.fetchall()]

    for prtn in prtn_arr:
        cursor.execute(rangeCmd.format(prtn, ratingMinValue, ratingMaxValue))
        execCmd_output_arr = cursor.fetchall()
        execCmd_output_arr = [['RangeRatingsPart{}{}'.format(prtn, i)] + list(currQuery) for i, currQuery in enumerate(execCmd_output_arr)]
        output_arr.append(execCmd_output_arr)

    cursor.execute(countCmd)
    rrobin_arr = cursor.fetchall()[0][0]
    selectCmd = 'SELECT * FROM roundrobinratingspart{0} WHERE rating>={1} and rating<={2};'

    for i in range(0,rrobin_arr):
        cursor.execute(selectCmd.format(i, ratingMinValue, ratingMaxValue))
        execCmd_output_arr = cursor.fetchall()
        execCmd_output_arr = [['RoundRobinRatingsPart{}'.format(i)] + list(currQuery) for i, currQuery in enumerate(execCmd_output_arr)]
        output_arr.append(execCmd_output_arr)

    writeToFile('RangeQueryOut.txt', output_arr)

def PointQuery(ratingsTableName, ratingValue, openconnection):

    output_arr = []
    cursor = openconnection.cursor()

    rangeCmd = 'SELECT * FROM rangeratingspart{0} WHERE rating={1};'
    countCmd = 'SELECT partitionnum FROM roundrobinratingsmetadata;'

    execCmd = 'SELECT partitionnum FROM rangeratingsmetadata WHERE maxrating>={0} AND minrating<={0};'.format(ratingValue)
    cursor.execute(execCmd)

    prtn_arr = [prtn[0] for prtn in cursor.fetchall()]

    for prtn in prtn_arr:
        cursor.execute(rangeCmd.format(prtn, ratingValue))
        execCmd_output_arr = cursor.fetchall()
        execCmd_output_arr = [['RangeRatingsPart{}{}'.format(prtn, i)] + list(currQuery) for i, currQuery in enumerate(execCmd_output_arr)]
        output_arr.append(execCmd_output_arr)

    cursor.execute(countCmd)
    rrobin_arr = cursor.fetchall()[0][0]

    selectCmd = 'SELECT * FROM roundrobinratingspart{0} WHERE rating={1};'

    for i in range(0, rrobin_arr):
        cursor.execute(selectCmd.format(i, ratingValue))
        execCmd_output_arr = cursor.fetchall()
        execCmd_output_arr = [['RoundRobinRatingsPart{}'.format(i)] + list(currQuery) for currQuery in execCmd_output_arr]
        output_arr.append(execCmd_output_arr)

    writeToFile('PointQueryOut.txt', output_arr)

def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()

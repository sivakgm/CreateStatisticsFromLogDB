/*
 * grossStatistics.cpp
 *
 *  Created on: Nov 20, 2014
 *      Author: sivaprakash
 */

#include <string>
#include <iostream>
#include "DBConnection.h"
#include "RowData.h"
#include "RowDataDenied.h"

extern DBConnection *statLog;

void updateRowDataAcc(ResultSet *dRes,ResultSet *ymRes,PreparedStatement *pstmt);
void insertRowDataAcc(ResultSet *ymRes,PreparedStatement *pstmt);

void updateRowDataDen(ResultSet *dRes,ResultSet *ymRes,PreparedStatement *pstmt);
void insertRowDataDen(ResultSet *ymRes,PreparedStatement *pstmt);

void grossStatisticsAcc(string tableName)
{
	PreparedStatement *readPstmt,*inPstmt,*upPstmt;
	ResultSet *dRes,*ymRes;


	string searchQueryMonth = "select * from "+ statLog->tableNameMonthAcc +"  where user=? and domain=? ;";
	string searchQueryYear = "select * from "+ statLog->tableNameYearAcc +"  where user=? and domain=?;";

	string insertMonth = "insert into " + statLog->tableNameMonthAcc + "(user,domain,size,connection,hit,miss,reponse_time) values(?,?,?,?,?,?,?);";
	string updateMonth = "update " + statLog->tableNameMonthAcc + " set size=?,connection=?,hit=?,miss=?,reponse_time=? where user=? and domain=?;";

	string insertYear = "insert into " + statLog->tableNameYearAcc + "(user,domain,size,connection,hit,miss,reponse_time) values(?,?,?,?,?,?,?);";
	string updateYear = "update " + statLog->tableNameYearAcc + " set size=?,connection=?,hit=?,miss=?,reponse_time=? where user=? and domain=?;";

	string selectQuery = "select * from " + tableName +";";
	readPstmt = statLog->conn->prepareStatement(selectQuery);
	dRes = readPstmt->executeQuery();



	while(dRes->next())
	{
		for(int i=0;i<2;i++)
		{
			if(i ==  0)
			{
				readPstmt = statLog->conn->prepareStatement(searchQueryMonth);
				inPstmt =  statLog->conn->prepareStatement(insertMonth);
				upPstmt = statLog->conn->prepareStatement(updateMonth);
			}
			else
			{
				readPstmt = statLog->conn->prepareStatement(searchQueryYear);
				inPstmt =  statLog->conn->prepareStatement(insertYear);
				upPstmt = statLog->conn->prepareStatement(updateYear);
			}

			readPstmt->setString(1,dRes->getString(1));
			readPstmt->setString(2,dRes->getString(2));
			ymRes = readPstmt->executeQuery();

			if(ymRes->next())
			{
				updateRowDataAcc(dRes,ymRes,upPstmt);
			}
			else
			{
				insertRowDataAcc(dRes,inPstmt);
			}
		}
	}

}

void updateRowDataAcc(ResultSet *dRes,ResultSet *ymRes,PreparedStatement *pstmt)
{

	RowData *rowData = new RowData();
	rowData->user = dRes->getString(1);
	rowData->domain = dRes->getString(2);
	rowData->size = dRes->getDouble(3) + ymRes->getDouble(3);
	rowData->connection = dRes->getInt(4) + ymRes->getInt(4);
	rowData->hit = dRes->getDouble(5) + ymRes->getDouble(5);
	rowData->miss = dRes->getDouble(6) + ymRes->getDouble(6);
	rowData->respone_time = dRes->getDouble(7) + ymRes->getDouble(7);

	updateTableAcc(rowData,pstmt);

}
void insertRowDataAcc(ResultSet *dRes,PreparedStatement *pstmt)
{

	RowData *rowData = new RowData();
	rowData->user = dRes->getString(1);
	rowData->domain = dRes->getString(2);
	rowData->size = dRes->getDouble(3);
	rowData->connection = dRes->getInt(4);
	rowData->hit = dRes->getDouble(5) ;
	rowData->miss = dRes->getDouble(6);
	rowData->respone_time = dRes->getDouble(7);

	insertIntoTableAcc(rowData,pstmt);
}

void grossStatisticsDen(string tableName)
{
	PreparedStatement *readPstmt,*inPstmt,*upPstmt;
	ResultSet *dRes,*ymRes;


	string searchQueryMonth = "select * from "+ statLog->tableNameMonthDen +"  where user=? and domain=? ;";
	string searchQueryYear = "select * from "+ statLog->tableNameYearDen +"  where user=? and domain=?;";

	string insertMonth = "insert into " + statLog->tableNameMonthDen + "(user,domain,connection) values(?,?,?);";
	string updateMonth = "update " + statLog->tableNameMonthDen + " set connection=? where user=? and domain=?;";

	string insertYear = "insert into " + statLog->tableNameYearDen + "(user,domain,connection) values(?,?,?);";
	string updateYear = "update " + statLog->tableNameYearDen + " set connection=? where user=? and domain=?;";

	string selectQuery = "select * from " + tableName +";";
	readPstmt = statLog->conn->prepareStatement(selectQuery);
	dRes = readPstmt->executeQuery();



	while(dRes->next())
	{
		for(int i=0;i<2;i++)
		{
			if(i ==  0)
			{
				readPstmt = statLog->conn->prepareStatement(searchQueryMonth);
				inPstmt =  statLog->conn->prepareStatement(insertMonth);
				upPstmt = statLog->conn->prepareStatement(updateMonth);
			}
			else
			{
				readPstmt = statLog->conn->prepareStatement(searchQueryYear);
				inPstmt =  statLog->conn->prepareStatement(insertYear);
				upPstmt = statLog->conn->prepareStatement(updateYear);
			}

			readPstmt->setString(1,dRes->getString(1));
			readPstmt->setString(2,dRes->getString(2));
			ymRes = readPstmt->executeQuery();

			if(ymRes->next())
			{
				updateRowDataDen(dRes,ymRes,upPstmt);
			}
			else
			{
				insertRowDataDen(dRes,inPstmt);
			}
		}
	}

}

void updateRowDataDen(ResultSet *dRes,ResultSet *ymRes,PreparedStatement *pstmt)
{

	RowDataDenied *rowData = new RowDataDenied();
	rowData->user = dRes->getString(1);
	rowData->domain = dRes->getString(2);
	rowData->connection = dRes->getInt(4) + ymRes->getInt(3);

	updateTableDen(rowData,pstmt);

}
void insertRowDataDen(ResultSet *dRes,PreparedStatement *pstmt)
{

	RowDataDenied *rowData = new RowDataDenied();
	rowData->user = dRes->getString(1);
	rowData->domain = dRes->getString(2);
	rowData->connection = dRes->getInt(3);


	insertIntoTableDen(rowData,pstmt);
}

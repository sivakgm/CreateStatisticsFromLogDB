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
#include "grossStatistics.h"

extern DBConnection *statLog;

//void updateRowDataAcc(ResultSet *dRes,ResultSet *ymRes,PreparedStatement *pstmt);
//void insertRowDataAcc(ResultSet *ymRes,PreparedStatement *pstmt);

//void updateRowDataDen(ResultSet *dRes,ResultSet *ymRes,PreparedStatement *pstmt);
//void insertRowDataDen(ResultSet *ymRes,PreparedStatement *pstmt);

void grossStatisticsAcc(string tableName)
{
	PreparedStatement *readPstmt,*inPstmt,*upPstmt;
	ResultSet *dRes,*ymRes;
	Statement *stmt = statLog->conn->createStatement();

	string year = tableName.substr(13,4);
	string month = tableName.substr(10,2);
	string day = tableName.substr(7,2);

	string yearStatisticstable = "ud_acc_"+year;
	string monthStatisticstable = "ud_acc_"+month;

	checkPresenecOfGrossStatisticsTableAcc(stmt,yearStatisticstable);
	checkPresenecOfGrossStatisticsTableAcc(stmt,monthStatisticstable);

	string searchQueryMonth = "select * from "+ monthStatisticstable +"  where user=? and domain=? ;";
	string searchQueryYear = "select * from "+ yearStatisticstable +"  where user=? and domain=?;";

	string insertMonth = "insert into " + monthStatisticstable + "(user,domain,size,connection,hit,miss,response_time) values(?,?,?,?,?,?,?);";
	string updateMonth = "update " + monthStatisticstable + " set size=?,connection=?,hit=?,miss=?,response_time=? where user=? and domain=?;";

	string insertYear = "insert into " + yearStatisticstable + "(user,domain,size,connection,hit,miss,response_time) values(?,?,?,?,?,?,?);";
	string updateYear = "update " + yearStatisticstable + " set size=?,connection=?,hit=?,miss=?,response_time=? where user=? and domain=?;";

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
	rowData->response_time = dRes->getDouble(7) + ymRes->getDouble(7);

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
	rowData->response_time = dRes->getDouble(7);

	insertIntoTableAcc(rowData,pstmt);
}

void checkPresenecOfGrossStatisticsTableAcc(Statement *stmt,string tableName)
{
	stmt->execute("create table if not exists " + tableName + "(user varchar(12),domain varchar(100), size double, connection int, hit float, miss float,response_time float);");
}

void grossStatisticsDen(string tableName)
{
	PreparedStatement *readPstmt,*inPstmt,*upPstmt;
	ResultSet *dRes,*ymRes;
	Statement *stmt = statLog->conn->createStatement();

	string year = tableName.substr(13,4);
	string month = tableName.substr(10,2);
	string day = tableName.substr(7,2);

	string yearStatisticstable = "ud_den_"+year;
	string monthStatisticstable = "ud_den_"+month;

	checkPresenecOfGrossStatisticsTableDen(stmt,yearStatisticstable);
	checkPresenecOfGrossStatisticsTableDen(stmt,monthStatisticstable);

	string searchQueryMonth = "select * from "+ monthStatisticstable +"  where user=? and domain=? ;";
	string searchQueryYear = "select * from "+ yearStatisticstable +"  where user=? and domain=?;";

	string insertMonth = "insert into " + monthStatisticstable + "(user,domain,connection) values(?,?,?);";
	string updateMonth = "update " + monthStatisticstable + " set connection=? where user=? and domain=?;";

	string insertYear = "insert into " + yearStatisticstable + "(user,domain,connection) values(?,?,?);";
	string updateYear = "update " + yearStatisticstable + " set connection=? where user=? and domain=?;";

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
	rowData->connection = dRes->getInt(3) + ymRes->getInt(3);

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

void checkPresenecOfGrossStatisticsTableDen(Statement *stmt,string tableName)
{
	stmt->execute("create table if not exists " + tableName + "(user varchar(12),domain varchar(100), connection int);");
}

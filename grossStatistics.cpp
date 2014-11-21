/*
 * grossStatistics.cpp
 *
 *  Created on: Nov 20, 2014
 *      Author: sivaprakash
 */

#include <string>
#include <iostream>
#include "DBConnection.h"

extern DBConnection *statLog;

void updateRowData(ResultSet *dRes,ResultSet *ymRes);
void insertRowData();

void grossStatisticsAcc(string tableName)
{
	PreparedStatement *readPstmt,*inPstmt,*upPstmt;
	ResultSet *res,*resgross;


	string searchQueryMonth = "select * from "+ statLog->tableNameMonthAcc +" where user=? and domain=?;";
	string searchQueryYear = "select * from "+ statLog->tableNameYearAcc +" where user=? and domain=?;";

	string selectQuery = "select * from " + tableName +";";
	readPstmt = statLog->conn->prepareStatement(selectQuery);
	res = readPstmt->executeQuery();

	while(res->next())
	{
		for(int i=0;i<2;i++)
		{
			if(i ==  0)
			{
				readPstmt=statLog->conn->prepareStatement(searchQueryMonth);
			}
			else
			{
				readPstmt=statLog->conn->prepareStatement(searchQueryMonth);
			}
			readPstmt->setString(1,res->getString(1));
			readPstmt->setString(2,res->getString(2));
			resgross = readPstmt->executeQuery();

			if(resgross->next())
			{
				updateRowData(resgross,res);
			}
			else
			{
				insertRowData();
			}
		}
	}

}

void updateRowData(ResultSet *dRes,ResultSet *ymRes)
{
	double size = dRes->getDouble(3) + ymRes->getDouble(3);
	int conn = dRes->getInt(4) + ymRes->getInt(4);
	double hit = dRes->getDouble(5) + ymRes->getDouble(5);
	double miss = dRes->getDouble(6) + ymRes->getDouble(6);
	double reponse_time = dRes->getDouble(7) + ymRes->getDouble(7);
}

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

void grossStatisticsAcc(string tableName)
{
	PreparedStatement *pstmt;
	ResultSet *res,*resgross;
	string selectQuery = "select * from " + tableName +";";
	string searchQueryMonth = "select * from ? where user=? and domain=?;";
	pstmt = statLog->conn->prepareStatement(selectQuery);
	res = pstmt->executeQuery();

	pstmt=statLog->conn->prepareStatement(searchQueryMonth);

	while(res->next())
	{
		for(int i=0;i<2;i++)
		{
			if(i ==  0)
			{
				pstmt->setString(1,statLog->tableNameMonth);
			}
			else
			{
				pstmt->setString(1,statLog->tableNameYear);
			}
			pstmt->setString(2,res->getString(1));
			pstmt->setString(3,res->getString(2));
			resgross = pstmt->executeQuery();
			if(resgross->next())
			{

			}
			else
			{

			}
		}
	}

}



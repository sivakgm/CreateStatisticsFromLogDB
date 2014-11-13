/*
 * main.cpp
 *
 *  Created on: Oct 29, 2014
 *      Author: sivaprakash
 */



#include "DBConnection.h"
#include "RowData.h"

int objCount;

int main()
{
	cout<<"Started the program\n";

	DBConnection *squidLog = new DBConnection();
	squidLog->dbConnOpen("127.0.0.1","3306","root","simple","squid");
	squidLog->tableName = "access_log";
	squidLog->readTable();


	DBConnection *statLog = new DBConnection();
	statLog->dbConnOpen("127.0.0.1","3306","root","simple","squid");
	statLog->createStatTableName();

	createStatistics(squidLog,statLog);

	/*RowData *rowData = new RowData();
	rowData->user="user1";
	rowData->domain=".google.com";
	rowData->hit=9;
	rowData->miss=9;
	rowData->priority=6;
	rowData->connection=6;
	rowData->size=6;
	statLog->updateTable(rowData);
	statLog->insertIntoTable(rowData);
	statLog->readTable();
	readResSet(statLog);*/

	cout<<"\nProgram designed by Karthik\n";
	return 0;
}




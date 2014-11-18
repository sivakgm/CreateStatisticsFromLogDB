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
	squidLog->setReadPstmt(1,squidLog->tableName,"","");
	squidLog->readTable();


	DBConnection *statLog = new DBConnection();
	statLog->dbConnOpen("127.0.0.1","3306","root","simple","squid");


	createStatistics(squidLog,statLog);

	insertAllObjDataIntoTable(statLog);

	/*RowData *rowData = new RowData();
	rowData->user="user2";
	rowData->domain=".yahoo.com";
	rowData->hit=18;
	rowData->miss=18;
	rowData->priority=18;
	rowData->connection=18;
	rowData->size=18;*/

//	statLog->updateTableAcc(rowData);
//	statLog->updateTableDen(rowData);
//	statLog->insertIntoTableAcc(rowData);
//	statLog->insertIntoTableDen(rowData);

//	statLog->readTable(0,statLog->tableNameAcc,rowData->user,rowData->domain);
//	readResSet(statLog);

	cout<<"\nProgram designed by Karthik\n";
	return 0;
}




/*
 * main.cpp
 *
 *  Created on: Oct 29, 2014
 *      Author: sivaprakash
 */



#include "DBConnection.h"
#include "RowData.h"
#include "RowDataDenied.h"


void createStatistics(DBConnection *,DBConnection *);

const int MAXDENIEDOBJ = 20;
int NoDENOBJ;

const int MAXACCESSOBJ = 20;
int NoACCOBJ;

RowDataDenied *rowDataDen[MAXDENIEDOBJ];
RowData *rowDataAcc[MAXACCESSOBJ];


int main()
{
	cout<<"Started the program\n";

	DBConnection *squidLog = new DBConnection();
	squidLog->dbConnOpen("127.0.0.1","3306","root","simple","squid");
	squidLog->tableName = "access_log";
	cout<<"1\n";
	squidLog->setReadPstmt(1,squidLog->tableName,"","");
	squidLog->readTable();


	DBConnection *statLog = new DBConnection();
	statLog->dbConnOpen("127.0.0.1","3306","root","simple","squid");
	cout<<"4\n";


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

void createStatistics(DBConnection *squidLog,DBConnection *statLog)
{
	string logDate = "";
	string user;
	string domain;
	int pointObj,isnewLogInTable;
	cout<<"5"<<"\n";
	while(squidLog->res->next())
	{
		cout<<6<<"\n";
		user=squidLog->res->getString(6);
		domain=parseURLtoDomain(squidLog->res->getString(11));

		cout<<user<<"\t"<<domain<<"\n";

		if(squidLog->res->getString(3) != logDate )
		{
			//cout<<squidLog->res->getString(3);
			logDate = squidLog->res->getString(3);
			string temp = logDate;
			for(unsigned int x=0;x<temp.length();x++)
			{
				if(temp[x] == '-')
				{
					temp[x]='_';
				}
			}
		cout<<temp;
		statLog->createStatTableName(temp);

		}


		if(squidLog->res->getString(7) != "TCP_DENIED")
		{
			cout<<"tcp_miss\n";
			pointObj = checkDataInOBJ(NoACCOBJ,user,domain);

			cout<<pointObj<<"\n";

			if(pointObj != -1)
			{
				updateDataInObj(statLog,rowDataAcc[pointObj],squidLog->res);
			}
			else
			{
				cout<<NoACCOBJ<<"\n";
				if(NoACCOBJ<MAXACCESSOBJ)
				{
					createNewObj();
					pointObj = NoACCOBJ -1;
					cout<<"pointOBj="<<pointObj<<"\n";
				}
				else
				{
					pointObj = getLeastObjPriority();
					insertObjIntoTable(pointObj,statLog);
					emptyTheObj(pointObj);
				}

				isnewLogInTable = checkDataInTable(statLog,statLog->tableNameAcc,user,domain);
				cout<<isnewLogInTable<<"\n";

				if(isnewLogInTable == 1)
				{
					updateObjFromTable(pointObj,statLog->res);
					updateDataInObj(statLog,rowDataAcc[pointObj],squidLog->res);
				}
				else
				{
					updateDataInObj(statLog,rowDataAcc[pointObj],squidLog->res);
				}
			}
		}
		/*else
		{
			pointObj = checkDataInDenOBJ(NoDENOBJ,user,domain);

			if(pointObj != -1)
			{
					updateDataInDenObj(statLog,rowDataDen[pointObj],squidLog->res);
			}
			else
			{
				if(NoDENOBJ<MAXDENIEDOBJ)
				{
					createNewDenObj();
					pointObj = NoDENOBJ -1;
				}
				else
				{
					pointObj = getLeastDenObjPriority();
					insertDenObjIntoTable(pointObj,statLog);
					emptyTheDenObj(pointObj);
				}

				isnewLogInTable = checkDataInTable(statLog,statLog->tableNameDen,user,domain);

				if(isnewLogInTable == 1)
				{
					updateDenObjFromTable(pointObj,statLog->res);
					updateDataInDenObj(statLog,rowDataDen[pointObj],squidLog->res);
				}
				else
				{
					updateDataInDenObj(statLog,rowDataDen[pointObj],squidLog->res);
				}
			}
		}*/
	}

}


/*
 * main.cpp
 *
 *  Created on: Oct 29, 2014
 *      Author: sivaprakash
 */



#include "DBConnection.h"
#include "RowData.h"
#include "RowDataDenied.h"
#include "grossStatistics.h"

void createStatistics(DBConnection *,DBConnection *);

const int MAXDENIEDOBJ = 20;
int NoDENOBJ;

const int MAXACCESSOBJ = 20;
int NoACCOBJ;


RowDataDenied *rowDataDen[MAXDENIEDOBJ];
RowData *rowDataAcc[MAXACCESSOBJ];

DBConnection *statLog;


string logDate="",year="",month="",day="";

int main()
{

	//readConfFile();

	cout<<"Started the program\n";

	DBConnection *squidLog = new DBConnection();
	squidLog->dbConnOpen("127.0.0.1","3306","root","simple","squid");
	squidLog->tableName = "access_log";
	squidLog->setReadPstmt(1,squidLog->tableName,"","");
	squidLog->readTable();

	statLog = new DBConnection();
	createStatistics(squidLog,statLog);

	for(int i=0;i<NoACCOBJ;i++)
	{
		delete rowDataAcc[i];
	}
	for(int i=0;i<NoDENOBJ;i++)
	{
		delete rowDataDen[i];
	}

	grossStatistics(statLog->tableNameAcc);


	//writeConfFile();
	cout<<"End Of program \n";
	return 0;
}


/*void readConfFile()
{
	ifstream confFile("squidStatistics.conf");
	for(int i=0;i<5;i++)
	{
		confFile>>confFile[i];
	}
}

void writeConfFile()
{
	ofstream confFile("squidStatistics.conf");
	for(int i=0;i<5;i++)
	{
		confFile<<confFile[i]<<"\n";
	}
}*/

void createStatistics(DBConnection *squidLog,DBConnection *statLog)
{
	string user;
	string domain;
	int pointObj,isnewLogInTable;
	while(squidLog->res->next())
	{
		user=squidLog->res->getString(6);
		domain=parseURLtoDomain(squidLog->res->getString(11));
		//cout<<user<<"\t"<<domain<<"\n";

		if(squidLog->res->getString(3) != logDate )
		{

			logDate = squidLog->res->getString(3);
			string temp = logDate;
			for(unsigned int x=0;x<temp.length();x++)
			{
				if(temp[x] == '-')
				{
					temp[x]='_';
				}
			}

			if(year != logDate.substr(6,4))
			{
				year=logDate.substr(6,4);
				string dbName = "squidStatistics_"+year;
				statLog->dbConnOpen("127.0.0.1","3306","root","simple",dbName);
				statLog->createStatTable(1,year);
			}
			if(month != logDate.substr(3,2))
			{
				month = logDate.substr(3,2);
				statLog->createStatTable(0,month);
			}
			if(day != logDate.substr(0,2))
			{
				day = logDate.substr(0,2);
			}

			statLog->createStatTableName(temp);
		}


		if(squidLog->res->getString(7) != "TCP_DENIED")
		{
			pointObj = checkDataInOBJ(NoACCOBJ,user,domain);



			if(pointObj != -1)
			{
				updateDataInObj(statLog,rowDataAcc[pointObj],squidLog->res);
			}
			else
			{
				if(NoACCOBJ<MAXACCESSOBJ)
				{
					createNewObj();
					pointObj = NoACCOBJ -1;
				}
				else
				{
					pointObj = getLeastObjPriority();
					insertObjIntoTable(pointObj,statLog);
					emptyTheObj(pointObj);
				}

				isnewLogInTable = checkDataInTable(statLog,statLog->tableNameAcc,user,domain);

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
		else
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
		}
	}

	insertAllObjDataIntoTable(statLog);
	insertAllDenObjDataIntoTable(statLog);
}


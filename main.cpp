/*
 * main.cpp
 *
 *  Created on: Oct 29, 2014
 *      Author: sivaprakash
 */


#include <fstream>
#include <cstdlib>
#include <pthread.h>

#include "DBConnection.h"
#include "RowData.h"
#include "RowDataDenied.h"
#include "grossStatistics.h"
#include "DomainStatistics.h"
#include "UserStatistics.h"

void createStatistics(DBConnection *,DBConnection *);
void readConfFile();
void writeConfFile();

const int MAXDENIEDOBJ = 20;
int NoDENOBJ;

const int MAXACCESSOBJ = 20;
int NoACCOBJ;

unsigned long int tabIndex;
string dd;


RowDataDenied *rowDataDen[MAXDENIEDOBJ];
RowData *rowDataAcc[MAXACCESSOBJ];

DBConnection *statLog;


string logDate="",year="",month="",day="";

int main()
{
	try
	{
		readConfFile();
		cout<<"Started the program\n";
		//cout<<tabIndex;

		DBConnection *squidLog = new DBConnection();
		squidLog->dbConnOpen("127.0.0.1","3306","root","simple","squid");

		string logReadQuery = "select * from access_log_new where id > ?;";
		PreparedStatement *pstm = squidLog->conn->prepareStatement(logReadQuery);
		pstm->setInt(1,tabIndex);
		squidLog->res = pstm->executeQuery();


	/*DBConnection *squidLog = new DBConnection();
	squidLog->dbConnOpen("127.0.0.1","3306","root","simple","squid");

	squidLog->tableName = "access_log";
	squidLog->setReadPstmt(1,squidLog->tableName,"","");
	squidLog->readTable();*/


		statLog = new DBConnection();
		createStatistics(squidLog,statLog);


	/*grossStatisticsAcc(statLog->tableNameAcc);
	grossStatisticsDen(statLog->tableNameDen);
	createDomainStatisticsAcc(statLog->tableNameAcc);
	createUserStatisticsAcc(statLog->tableNameAcc);
	createDomainStatisticsDen(statLog->tableNameDen);
	createUserStatisticsDen(statLog->tableNameDen);*/

		writeConfFile();
		cout<<"End Of program \n";
		pthread_exit(NULL);
	}
	catch (sql::SQLException &e)
	{
		cout << "# ERR: SQLException in " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
		cout << "# ERR: " << e.what();
		cout << " (MySQL error code: " << e.getErrorCode();
		cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	}
	catch (exception& e)
	{
		cout << "# ERR File: " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	    cout << e.what() << '\n';
	}
		return 0;
}


void readConfFile()
{
	try
	{
		ifstream confFile("/home/sivaprakash/workspace/StatisticsDataFromDB/src/squ.conf");
		confFile>>tabIndex>>dd;
	}
	catch (exception& e)
	{
		cout << "# ERR File: " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	    cout << e.what() << '\n';
	}
}

void writeConfFile()
{
	try
	{
		ofstream confFile("/home/sivaprakash/workspace/StatisticsDataFromDB/src/squ.conf");
		confFile<<tabIndex<<"\t"<<dd;
	}
	catch (exception& e)
	{
		cout << "# ERR File: " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	    cout << e.what() << '\n';
	}
}

void createStatistics(DBConnection *squidLog,DBConnection *statLog)
{
	try
	{
		string user;
		string domain;
		string temp ;
		int pointObj,isnewLogInTable;
		while(squidLog->res->next())
		{
			user=squidLog->res->getString(6);
			domain=parseURLtoDomain(squidLog->res->getString(11));
	//	cout<<user<<"\t"<<domain<<"\n";

			if(squidLog->res->getString(3) != logDate )
			{

				logDate = squidLog->res->getString(3);
				temp = logDate;
				for(unsigned int x=0;x<temp.length();x++)
				{
					if(temp[x] == '-')
					{
						temp[x]='_';
					}
				}

				//Checking whether the lastly generated date in squ.conf is at present date

				if( dd != temp && dd != "a")
				{
					pthread_t lastDay[2];
					cout<<"Entering day log";

					string oldtn = "ud_acc_"+dd;
					ofstream confFile("/home/sivaprakash/workspace/StatisticsDataFromDB/src/tabAcc.conf");
					confFile<<oldtn;
					confFile.close();
					pthread_create(&lastDay[0], NULL,grossStatisticsAcc,(void *)statLog->tableNameAcc.c_str());

					oldtn = "ud_den_"+dd;
					ofstream confFile1("/home/sivaprakash/workspace/StatisticsDataFromDB/src/tabDen.conf");
					confFile1<<oldtn;
					confFile1.close();
					pthread_create(&lastDay[1], NULL,grossStatisticsDen,(void *)statLog->tableNameDen.c_str());



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
					if(day != "")
					{

						insertAllObjDataIntoTable(statLog);
						pthread_t thread[2];
						//cout<<"\n\n"<<"day:"<<day<<"\n\n";
						ofstream confFile("/home/sivaprakash/workspace/StatisticsDataFromDB/src/tabAcc.conf");
						confFile<<statLog->tableNameAcc;
						confFile.close();
						pthread_create(&thread[0], NULL,grossStatisticsAcc,(void *)statLog->tableNameAcc.c_str());

						insertAllDenObjDataIntoTable(statLog);

						ofstream confFile1("/home/sivaprakash/workspace/StatisticsDataFromDB/src/tabDen.conf");
						confFile1<<statLog->tableNameDen;
						confFile1.close();
						pthread_create(&thread[1], NULL,grossStatisticsDen,(void *)statLog->tableNameDen.c_str());






					}
					day = logDate.substr(0,2);
				}
			//sleep(2);
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
			tabIndex = squidLog->res->getInt(1);
			dd = temp;
		}

//	cout<<"ma\n";
		insertAllObjDataIntoTable(statLog);
		insertAllDenObjDataIntoTable(statLog);
	}
	catch (sql::SQLException &e)
	{
		cout << "# ERR: SQLException in " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
		cout << "# ERR: " << e.what();
		cout << " (MySQL error code: " << e.getErrorCode();
		cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	}
	catch (exception& e)
	{
		cout << "# ERR File: " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	    cout << e.what() << '\n';
	}

}


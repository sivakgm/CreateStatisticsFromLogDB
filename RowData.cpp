/*
 * RowData.cpp
 *
 *  Created on: Nov 5, 2014
 *      Author: sivaprakash
 */

#include "RowData.h"
#include "DBConnection.h"

const int MAXACCESSOBJ = 15;
int NoACCOBJ;
const int MAXDENIEDOBJ = 5;
int NoDENOBJ;

RowData *rowDataAcc[MAXACCESSOBJ];
RowData *rowDataDen[MAXDENIEDOBJ];

RowData::RowData(void)
{
	user = "";
	domain = "";
	connection = 0;
	miss = 0.0;
	hit = 0.0;
	size = 0.0;
	priority = 1;
	isInTable = 0;
}

/*RowData::~RowData() {
	// TODO Auto-generated destructor stub
}*/

void createStatistics(DBConnection *squidLog,DBConnection *statLog)
{
	while(squidLog->res->next())
	{
		int pointObj,isnewLogInTable;
		string user=squidLog->res->getString(6);
		string domain=parseURLtoDomain(squidLog->res->getString(11));

		if(squidLog->res->getString(7) != "TCP_DENIED")
		{
			pointObj = checkDataInOBJ(NoACCOBJ,user,domain);

			if(pointObj != NULL)
			{
				updateDataInObj(rowDataAcc[pointObj],squidLog->res);
			}
			else
			{
				if(NoACCOBJ<MAXACCESSOBJ)
				{
					createNewObj();
					updateDataInObj(rowDataAcc[NoACCOBJ-1],squidLog->res);
				}
				else
				{
					pointObj = getLeastObjPriority();
					insertLeastUsedObjIntoTable(pointObj,statLog);
					emptyTheObj(pointObj);

					isnewLogInTable = checkDataInTable(statLog,statLog->tableNameAcc,user,domain);
					if(isnewLogInTable == 1)
					{
						updateObjFromTable(pointObj,statLog->res);
						updateDataInObj(rowDataAcc[pointObj],squidLog->res);
					}
					else
					{
						updateDataInObj(rowDataAcc[pointObj],squidLog->res);
					}
				}
			}

			RowData *rowData1 = new RowData();
			rowData1->domain = parseURLtoDomain(squidLog->res->getString(11));
			cout<<rowData1->domain;
		}
	}
}

void setObjPriority()
{

}

int getLeastObjPriority()
{
	return 1;
}


void createNewObj()
{
	rowDataAcc[NoACCOBJ] = new RowData();
	NoACCOBJ++;
	return;
}

void emptyTheObj(int pointObj)
{
		rowDataAcc[pointObj]->user = "";
		rowDataAcc[pointObj]->domain = "";
		rowDataAcc[pointObj]->connection = 0;
		rowDataAcc[pointObj]->miss = 0.0;
		rowDataAcc[pointObj]->hit = 0.0;
		rowDataAcc[pointObj]->size = 0.0;
		rowDataAcc[pointObj]->priority = 1;
		rowDataAcc[pointObj]->isInTable = 0;
}

void updateObjFromTable(int pointObj,ResultSet *res)
{
	rowDataAcc[pointObj]->user = res->getString(1);
	rowDataAcc[pointObj]->domain = res->getString(2);
	rowDataAcc[pointObj]->connection = res->getInt(4);
	rowDataAcc[pointObj]->miss = res->getDouble(6);
	rowDataAcc[pointObj]->hit =  res->getDouble(5);
	rowDataAcc[pointObj]->size = res->getDouble(3);
	rowDataAcc[pointObj]->priority = 1;
	rowDataAcc[pointObj]->isInTable = 1;
}

void insertLeastUsedObjIntoTable(int pointObj,DBConnection *statLog)
{
	if(rowDataAcc[pointObj]->isInTable == 1)
	{
		statLog->updateTableAcc(rowDataAcc[pointObj]);
	}
	else
	{
		statLog->insertIntoTableAcc(rowDataAcc[pointObj]);
	}
}

void updateDataInObj(RowData *rowdata,ResultSet *res)
{
	rowdata->user = res->getString(7);
	rowdata->domain = parseURLtoDomain(res->getString(11));
	rowdata->connection = rowdata->connection + 1;
	rowdata->size = rowdata->size + res->getDouble(9);
	rowdata->priority = 1;
	if(res->getString(7) == "TCP_HIT" || res->getString(7) == "TCP_MEM_HIT" || res->getString(7) == "UDP_HIT" || res->getString(7) == "UDP_HIT_OBJ")
	{
		rowdata->hit = 0.0;
	}
	else
	{
		rowdata->miss = 0.0;
	}
	setObjPriority();
	return;
}

int checkDataInTable(DBConnection *statLog,string tableName,string user,string domain)
{
	statLog->readTable(0,tableName,user,domain);
	if(statLog->res->next())
	{
		return 1;
	}
	return NULL;
}

int checkDataInOBJ(int count,string user,string domain)
{
	for(int i=0;i<count;i++)
	{
		if(rowDataAcc[i]->user == user && rowDataAcc[i]->domain == domain)
		{
			return rowDataAcc[i];
		}
	}

	return NULL;
}

string parseURLtoDomain(string url)
{
		string delimiters = "/";
		size_t current = 0;
		size_t next = 0;
		string domain = url;

		next = url.find_first_of( delimiters, current );

		if(next != string::npos)
		{
			if( url[next + 1] == '/')
			{
				current = next + 2;
				next = url.find_first_of( delimiters, current );
				domain = url.substr( current, next - current );
			}
			else
			{
				domain = url.substr( current, next );
			}
		}

	return domain;
}

void readResSet(DBConnection *logDB)
{
	while(logDB->res->next())
	{
		cout<<"\t"<<logDB->res->getString(1);
		cout<<"\t"<<logDB->res->getString(2);
		cout<<"\t"<<logDB->res->getDouble(3);
		cout<<"\t"<<logDB->res->getInt(4);
		cout<<"\t"<<logDB->res->getDouble(5);
		cout<<"\t"<<logDB->res->getDouble(6)<<"\n";

		/*cout<<"\t"<<logDB->res->getString(1);
		cout<<"\t"<<logDB->res->getString(2);
		cout<<"\t"<<logDB->res->getInt(3)<<endl;*/

	}
	return;
}

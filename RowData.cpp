/*
 * RowData.cpp
 *
 *  Created on: Nov 5, 2014
 *      Author: sivaprakash
 */

#include "RowData.h"
#include "DBConnection.h"

extern const int MAXACCESSOBJ = 20;
extern int NoACCOBJ;

extern RowData *rowDataAcc[MAXACCESSOBJ];

RowData::RowData(void)
{
	user = "";
	domain = "";
	connection = 0;
	miss = 0.0;
	hit = 0.0;
	size = 0.0;
	response_time = 0.0;
	priority = 0;
	isInTable = 0;
}

/*RowData::~RowData() {
	// TODO Auto-generated destructor stub
}*/

void insertAllObjDataIntoTable(DBConnection *statLog)
{
	cout<<"test\n";
	for(int i=0;i<NoACCOBJ;i++)
	{
		insertObjIntoTable(i,statLog);
	}
	for(int i=0;i<NoACCOBJ;i++)
	{
			delete rowDataAcc[i];
	}
	NoACCOBJ = 0;
}

void setObjPriority(int lim)
{
	if(lim == 0)
	{
		lim = NoACCOBJ;
	}
	for(int i=0;i<lim;i++)
	{
		rowDataAcc[i]->priority = rowDataAcc[i]->priority + 1;
	}
}

int getLeastObjPriority()
{
	for(int i=0;i<NoACCOBJ;i++)
	{
		if(rowDataAcc[i]->priority == NoACCOBJ )
			return i;
	}
	return -1;
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
		rowDataAcc[pointObj]->isInTable = 0;
		rowDataAcc[pointObj]->response_time = 0;
}

void updateObjFromTable(int pointObj,ResultSet *res)
{
	rowDataAcc[pointObj]->user = res->getString(1);
	rowDataAcc[pointObj]->domain = res->getString(2);
	rowDataAcc[pointObj]->connection = res->getInt(4);
	rowDataAcc[pointObj]->miss = res->getDouble(6);
	rowDataAcc[pointObj]->hit =  res->getDouble(5);
	rowDataAcc[pointObj]->size = res->getDouble(3);
	rowDataAcc[pointObj]->response_time = res->getDouble(7);
	rowDataAcc[pointObj]->isInTable = 1;
}

void insertObjIntoTable(int pointObj,DBConnection *statLog)
{
	if(rowDataAcc[pointObj]->isInTable == 1)
	{
		updateTableAcc(rowDataAcc[pointObj],statLog->upPstmtAcc);
	}
	else
	{
		insertIntoTableAcc(rowDataAcc[pointObj],statLog->insPstmtAcc);
	}
}

void updateDataInObj(DBConnection *statLog,RowData *rowdata,ResultSet *res)
{
	int lim = rowdata->priority;
	rowdata->user = res->getString(6);
	rowdata->domain = parseURLtoDomain(res->getString(11));
	rowdata->connection = rowdata->connection + 1;
	rowdata->size = rowdata->size + res->getDouble(9);
	rowdata->response_time = rowdata->response_time + res->getDouble(5);
	rowdata->priority = 0;
	if(res->getString(7) == "TCP_HIT" || res->getString(7) == "TCP_MEM_HIT" || res->getString(7) == "UDP_HIT" || res->getString(7) == "UDP_HIT_OBJ")
	{
		rowdata->hit = rowdata->hit + res->getDouble(9);
	}
	else
	{
		rowdata->miss = rowdata->miss + res->getDouble(9) ;
	}
	insertIntoTableAccTime(rowdata,res->getString(4),statLog->insPstmtAccTime);
	setObjPriority(lim);
	return;
}


int checkDataInOBJ(int count,string user,string domain)
{
	for(int i=0;i<count;i++)
	{
		if(rowDataAcc[i]->user == user && rowDataAcc[i]->domain == domain)
		{
			return i;
		}
	}
	return -1;
}


/*void readResSet(DBConnection *logDB)
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
		cout<<"\t"<<logDB->res->getInt(3)<<endl;

	}
	return;
}*/


int checkDataInTable(DBConnection *statLog,string tableName,string user,string domain)
{
	statLog->setReadPstmt(0,tableName,user,domain);
	statLog->readTable();
	if(statLog->res->next())
	{
		return 1;
	}
	return -1;
}

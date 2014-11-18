/*
 * RowDataDenied.cpp
 *
 *  Created on: Nov 18, 2014
 *      Author: sivaprakash
 */

#include "RowDataDenied.h"
#include "DBConnection.h"


extern const int MAXDENIEDOBJ = 20;
extern int NoDENOBJ;

extern RowDataDenied *rowDataDen[MAXDENIEDOBJ];


RowDataDenied::RowDataDenied()
{
	user = "";
	domain = "";
	connection = 0;
	priority = 0;
	isInTable = 0;
}


void updateDenObjFromTable(int pointObj,ResultSet *res)
{
	rowDataDen[pointObj]->user = res->getString(1);
	rowDataDen[pointObj]->domain = res->getString(2);
	rowDataDen[pointObj]->connection = res->getInt(3);
	rowDataDen[pointObj]->isInTable = 1;
}

void insertDenObjIntoTable(int pointObj,DBConnection *statLog)
{
	if(rowDataDen[pointObj]->isInTable == 1)
	{
		statLog->updateTableDen(rowDataDen[pointObj]);
	}
	else
	{
		statLog->insertIntoTableDen(rowDataDen[pointObj]);
	}
}

void emptyTheDenObj(int pointObj)
{
		rowDataDen[pointObj]->user = "";
		rowDataDen[pointObj]->domain = "";
		rowDataDen[pointObj]->connection = 0;
		rowDataDen[pointObj]->isInTable = 0;
}


int getLeastDenObjPriority()
{
	for(int i=0;i<NoDENOBJ;i++)
	{
		if(rowDataDen[i]->priority == NoDENOBJ )
			return i;
	}
	return -1;
}

void setDenObjPriority(int lim)
{
	if(lim == 0)
	{
		lim = NoDENOBJ;
	}
	for(int i=0;i<lim;i++)
	{
		rowDataDen[i]->priority = rowDataDen[i]->priority + 1;
	}
}

void updateDataInDenObj(DBConnection *statLog,RowDataDenied *rowdataden,ResultSet *res)
{

	int lim = rowdataden->priority;
	rowdataden->user = res->getString(6);
	rowdataden->domain = parseURLtoDomain(res->getString(11));
	rowdataden->connection = rowdataden->connection + 1;
	rowdataden->priority = 0;

	statLog->insertIntoTableDenTime(rowdataden,res->getString(4));
	setDenObjPriority(lim);
	return;
}

int checkDataInDenOBJ(int count,string user,string domain)
{
	for(int i=0;i<count;i++)
	{
		if(rowDataDen[i]->user == user && rowDataDen[i]->domain == domain)
		{
			return i;
		}
	}
	return -1;
}
void insertAllDenObjDataIntoTable(DBConnection *statLog)
{
	for(int i=0;i<NoDENOBJ;i++)
	{
		insertDenObjIntoTable(i,statLog);
	}
}

void createNewDenObj()
{
	rowDataDen[NoDENOBJ] = new RowDataDenied();
	NoDENOBJ++;
	return;
}

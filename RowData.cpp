/*
 * RowData.cpp
 *
 *  Created on: Nov 5, 2014
 *      Author: sivaprakash
 */

#include "RowData.h"
#include "DBConnection.h"

const int MAXACCESSOBJ = 15;
const int MAXDENIEDOBJ = 5;

RowData *rowDataAcc[MAXACCESSOBJ];
RowData *rowDataDen[MAXDENIEDOBJ];

/*RowData::RowData() {
	// TODO Auto-generated constructor stub

}

RowData::~RowData() {
	// TODO Auto-generated destructor stub
}*/

void createStatistics(DBConnection *squidLog,DBConnection *statLog)
{

	while(squidLog->res->next())
	{
		RowData *rowData1 = new RowData();
		rowData1->domain = parseURLtoDomain(squidLog->res->getString(11));
		cout<<rowData1->domain;
	}
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
	}
	return;
}

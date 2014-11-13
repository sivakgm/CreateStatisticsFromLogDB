/*
 * RowData.h
 *
 *  Created on: Nov 5, 2014
 *      Author: sivaprakash
 */


#ifndef ROWDATA_H_
#define ROWDATA_H_

#include <string>

class DBConnection;

using namespace std;


class RowData {
public:
	string user;
	string domain;
	float size;
	int connection;
	float hit;
	float miss;
	int priority;

	//RowData();
	//virtual ~RowData();

};

void createStatistics(DBConnection *squidLog);
void readResSet(DBConnection *logDB);
string parseURLtoDomain(string url);

#endif /* ROWDATA_H_ */

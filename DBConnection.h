/*
 * DBConnection.h
 *
 *  Created on: Nov 5, 2014
 *      Author: sivaprakash
 */

#ifndef DBCONNECTION_H_
#define DBCONNECTION_H_

#include <iostream>
#include <string>

#include <mysql_connection.h>
#include <mysql_driver.h>

#include <cppconn/driver.h>
#include <cppconn/statement.h>
#include <cppconn/resultset.h>
#include <cppconn/prepared_statement.h>
#include <cppconn/exception.h>

#include <boost/lexical_cast.hpp>

#include <ctime>
#include <sys/time.h>

class RowData;

using namespace std;
using namespace sql;


class DBConnection
{
public:

	Connection *conn;
	PreparedStatement *insPstmtAcc,*insPstmtDen;
	PreparedStatement *upPstmtAcc,*upPstmtDen;
	PreparedStatement *readpstmt;
	Statement *stmt;
	ResultSet *res;

	string tableName;
	string tableNameAcc,tableNameDen;


	void dbConnOpen(string host,string port,string user,string pass,string schema);

	void setPstmt();
	void setReadPstmt(int a,string tableName,string user,string domain);
	void insertIntoTableAcc(RowData *rowData);
	void updateTableAcc(RowData *rowData);
	void insertIntoTableDen(RowData *rowData);
	void updateTableDen(RowData *rowData);
	void readTable();

	void createStatTableName();
	bool createTableIfNotExist();

};

string timeAndDate();

#endif /* DBCONNECTION_H_ */

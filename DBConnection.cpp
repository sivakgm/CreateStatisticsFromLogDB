/*
 * DBConnection.cpp

 *
 *  Created on: Nov 5, 2014
 *      Author: sivaprakash
 *
 */

#include "DBConnection.h"
#include "RowDataDenied.h"
#include "RowData.h"

void DBConnection::createDBIfNotExists(string schema)
{
	try
	{
		this->stmt=this->conn->createStatement();
		this->stmt->execute("create database if not exists "+schema);
		return;
	}
	catch (sql::SQLException &e)
	{
	  cout << "# ERR: SQLException in " << __FILE__;
	  cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	  cout << "# ERR: " << e.what();
	  cout << " (MySQL error code: " << e.getErrorCode();
	  cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	}
}

void DBConnection::createStatTable(int flag,string tableName)
{
	try
	{
		this->stmt=this->conn->createStatement();
		if(tableName != "")
		{
			if(flag == 1)
			{
				this->tableNameYearAcc = "ud_acc_"+tableName;
				this->tableNameYearDen = "ud_den_"+tableName;
				this->stmt->execute("create table if not exists ud_acc_" + tableName + " (user varchar(12), domain varchar(100), size double, connection int, hit float, miss float,response_time float);");
				this->stmt->execute("create table if not exists ud_den_" + tableName + " (user varchar(12), domain varchar(100),connection int);");
			}
			else
			{
				this->tableNameMonthAcc = "ud_acc_"+tableName;
				this->tableNameMonthDen = "ud_den_"+tableName;
				this->stmt->execute("create table if not exists ud_acc_" + tableName + " (user varchar(12), domain varchar(100), size double, connection int, hit float, miss float,response_time float);");
				this->stmt->execute("create table if not exists ud_den_" + tableName + " (user varchar(12), domain varchar(100),connection int);");
			}
		}
	}
	catch (sql::SQLException &e)
	{
	  cout << "# ERR: SQLException in " << __FILE__;
	  cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	  cout << "# ERR: " << e.what();
	  cout << " (MySQL error code: " << e.getErrorCode();
	  cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	}
}

void DBConnection::dbConnOpen(string host,string port,string user,string pass,string schema)
{
	try
	{
		Driver *driver = get_driver_instance();
		string addre = "tcp://"+host+":"+port;
		this->conn = driver->connect(addre,user,pass);
		this->createDBIfNotExists(schema);
		this->conn->setSchema(schema);
		return;
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

void DBConnection::createStatTableName(string tableName)
{

	try
	{
		this->tableNameAcc = "ud_acc_"+tableName;
		this->tableNameDen = "ud_den_"+tableName;
		this->tableNameAccTime = "at_acc_" + tableName;
		this->tableNameDenTime= "at_den_" + tableName;
		this->createTableIfNotExist();
		this->setPstmt();
		return;
	}
	catch (sql::SQLException &e)
	{
	  cout << "# ERR: SQLException in " << __FILE__;
	  cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	  cout << "# ERR: " << e.what();
	  cout << " (MySQL error code: " << e.getErrorCode();
	  cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	}
}

void DBConnection::createTableIfNotExist()
{
	try
	{
		this->stmt=this->conn->createStatement();
		this->stmt->execute("create table if not exists " + this->tableNameAcc + " (user varchar(12), domain varchar(100), size double, connection int, hit float, miss float,response_time float);");
		this->stmt->execute("create table if not exists " + this->tableNameDen + " (user varchar(12), domain varchar(100), connection int);");
		this->stmt->execute("create table if not exists " + this->tableNameAccTime + " (user varchar(12), domain varchar(100), accTime varchar(15));");
		this->stmt->execute("create table if not exists " + this->tableNameDenTime + " (user varchar(12), domain varchar(100), accTime varchar(15));");
	}
	catch (sql::SQLException &e)
	{
	  cout << "# ERR: SQLException in " << __FILE__;
	  cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	  cout << "# ERR: " << e.what();
	  cout << " (MySQL error code: " << e.getErrorCode();
	  cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	}
}

void DBConnection::setPstmt()
{
	try
	{
		string query = "insert into " + this->tableNameAcc +"(user,domain,size,connection,hit,miss,response_time) values(?,?,?,?,?,?,?)";
		this->insPstmtAcc=this->conn->prepareStatement(query);

		query = "update " + this->tableNameAcc + " set size=?,connection=?,hit=?,miss=?,response_time=? where user=? and domain=?;";
		this->upPstmtAcc=this->conn->prepareStatement(query);

		query = "insert into " + this->tableNameDen +"(user,domain,connection) values(?,?,?)";
		this->insPstmtDen=this->conn->prepareStatement(query);

		query = "update " + this->tableNameDen + " set connection=? where user=? and domain=?;";
		this->upPstmtDen=this->conn->prepareStatement(query);

		query = "insert into " + this->tableNameAccTime +"(user,domain,accTime) values(?,?,?);";
		this->insPstmtAccTime=this->conn->prepareStatement(query);

		query = "insert into " + this->tableNameDenTime +"(user,domain,accTime) values(?,?,?);";
		this->insPstmtDenTime=this->conn->prepareStatement(query);
	}
	catch (sql::SQLException &e)
	{
		cout << "# ERR: SQLException in " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
		cout << "# ERR: " << e.what();
		cout << " (MySQL error code: " << e.getErrorCode();
		cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	}
}

void DBConnection::setReadPstmt(int flag,string tableName,string user,string domain)
{
	try
	{
		string query;

		if(flag == 1)
		{
			query = "select * from " + tableName +";";
		}
		else
		{
			query = "select * from " + tableName +" where user=? and domain=?;";
		}

		this->readpstmt = this->conn->prepareStatement(query);

		if(flag != 1 )
		{
			this->readpstmt->setString(1,user);
			this->readpstmt->setString(2,domain);
		}
	}
	catch (sql::SQLException &e)
	{
		cout << "# ERR: SQLException in " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	  	cout << "# ERR: " << e.what();
	  	cout << " (MySQL error code: " << e.getErrorCode();
	  	cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	}
}

string timeAndDate()
{
	try
	{
		time_t now = time(0);
		tm *ltm = localtime(&now);
		string date = boost::lexical_cast<std::string>((ltm->tm_mday < 10 ?"0":""))+boost::lexical_cast<std::string>(ltm->tm_mday)+"_"+boost::lexical_cast<std::string>(1 + ltm->tm_mon < 10 ?"0":"")+boost::lexical_cast<std::string>(1 + ltm->tm_mon)+"_"+boost::lexical_cast<std::string>(1900 + ltm->tm_year);
		return date;
	}
	catch (exception& e)
	{
		cout << "# ERR File: " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
		cout << e.what() << '\n';
	}
	return NULL;
}

void insertIntoTableAccTime(RowData *rowData,string acctime,PreparedStatement *pstmt)
{
	try
	{
		pstmt->setString(1,rowData->user);
		pstmt->setString(2,rowData->domain);
		pstmt->setString(3,acctime);
		pstmt->executeUpdate();
	}
	catch (sql::SQLException &e)
	{
		cout << "# ERR: SQLException in " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
		cout << "# ERR: " << e.what();
		cout << " (MySQL error code: " << e.getErrorCode();
		cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	}
}

void insertIntoTableDenTime(RowDataDenied *rowData,string acctime,PreparedStatement *pstmt)
{
	try
	{
		pstmt->setString(1,rowData->user);
		pstmt->setString(2,rowData->domain);
		pstmt->setString(3,acctime);
		pstmt->executeUpdate();
	}
	catch (sql::SQLException &e)
	{
		cout << "# ERR: SQLException in " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
		cout << "# ERR: " << e.what();
		cout << " (MySQL error code: " << e.getErrorCode();
		cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	}
}


void insertIntoTableAcc(RowData *rowData,PreparedStatement *pstmt)
{
	try
	{
		pstmt->setString(1,rowData->user);
		pstmt->setString(2,rowData->domain);
		pstmt->setDouble(3,rowData->size);
		pstmt->setInt(4,rowData->connection);
		pstmt->setDouble(5,rowData->hit);
		pstmt->setDouble(6,rowData->miss);
		pstmt->setDouble(7,rowData->response_time);
		pstmt->executeUpdate();
	}
	catch (sql::SQLException &e)
	{
		cout << "# ERR: SQLException in " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
		cout << "# ERR: " << e.what();
		cout << " (MySQL error code: " << e.getErrorCode();
		cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	}
}

void updateTableAcc(RowData *rowData,PreparedStatement *pstmt)
{
	try
	{
		pstmt->setDouble(1,rowData->size);
		pstmt->setInt(2,rowData->connection);
		pstmt->setDouble(3,rowData->hit);
		pstmt->setDouble(4,rowData->miss);
		pstmt->setDouble(5,rowData->response_time);
		pstmt->setString(6,rowData->user);
		pstmt->setString(7,rowData->domain);

		pstmt->executeUpdate();
		return;
	}
	catch (sql::SQLException &e)
	{
		cout << "# ERR: SQLException in " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
		cout << "# ERR: " << e.what();
		cout << " (MySQL error code: " << e.getErrorCode();
		cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	}
}

void insertIntoTableDen(RowDataDenied *rowData,PreparedStatement *pstmt)
{
	try
	{
		pstmt->setString(1,rowData->user);
		pstmt->setString(2,rowData->domain);
		pstmt->setInt(3,rowData->connection);
		pstmt->executeUpdate();
		return;
	}
	catch (sql::SQLException &e)
	{
		cout << "# ERR: SQLException in " << __FILE__;
	  	cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	  	cout << "# ERR: " << e.what();
	  	cout << " (MySQL error code: " << e.getErrorCode();
	  	cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	}
}

void updateTableDen(RowDataDenied *rowData,PreparedStatement *pstmt)
{
	try
	{
		pstmt->setInt(1,rowData->connection);
		pstmt->setString(2,rowData->user);
		pstmt->setString(3,rowData->domain);
		pstmt->executeUpdate();
		return;
	}
	catch (sql::SQLException &e)
	{
		cout << "# ERR: SQLException in " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
		cout << "# ERR: " << e.what();
		cout << " (MySQL error code: " << e.getErrorCode();
		cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	}
}

void DBConnection::readTable()
{
	try
	{
		this->res = this->readpstmt->executeQuery();
		return;
	}
	catch (sql::SQLException &e)
	{
		cout << "# ERR: SQLException in " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
		cout << "# ERR: " << e.what();
		cout << " (MySQL error code: " << e.getErrorCode();
		cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	}
}

string parseURLtoDomain(string url)
{
	try
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
	catch (exception& e)
	{
		cout << "# ERR File: " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	    cout << e.what() << '\n';
	}
	return NULL;
}

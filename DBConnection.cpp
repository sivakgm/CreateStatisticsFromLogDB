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


void DBConnection::dbConnOpen(string host,string port,string user,string pass,string schema)
{
	Driver *driver = get_driver_instance();
	string addre = "tcp://"+host+":"+port;
	this->conn = driver->connect(addre,user,pass);
	this->conn->setSchema(schema);
	return;
}

void DBConnection::createStatTableName(string tableName)
{
	this->tableNameAcc = "ud_acc_"+tableName;
	this->tableNameDen = "ud_den_"+tableName;
	this->tableNameAccTime = "at_acc_" + tableName;
	this->tableNameDenTime= "at_den_" + tableName;
	this->createTableIfNotExist();
	this->setPstmt();
	return;
}

void DBConnection::createTableIfNotExist()
{
	this->stmt=this->conn->createStatement();
	this->stmt->execute("create table if not exists " + this->tableNameAcc + " (user varchar(12), domain varchar(100), size double, connection int, hit float, miss float,reponse_time float);");
	this->stmt->execute("create table if not exists " + this->tableNameDen + " (user varchar(12), domain varchar(100), connection int);");
	this->stmt->execute("create table if not exists " + this->tableNameAccTime + " (user varchar(12), domain varchar(100), accTime varchar(15));");
	this->stmt->execute("create table if not exists " + this->tableNameDenTime + " (user varchar(12), domain varchar(100), accTime varchar(15));");
}

void DBConnection::setPstmt()
{

	string query = "insert into " + this->tableNameAcc +"(user,domain,size,connection,hit,miss,reponse_time) values(?,?,?,?,?,?,?)";
	this->insPstmtAcc=this->conn->prepareStatement(query);

	query = "update " + this->tableNameAcc + " set size=?,connection=?,hit=?,miss=?,reponse_time=? where user=? and domain=?;";
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

void DBConnection::setReadPstmt(int flag,string tableName,string user,string domain)
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

string timeAndDate()
{
	time_t now = time(0);
	tm *ltm = localtime(&now);
	string date = boost::lexical_cast<std::string>((ltm->tm_mday < 10 ?"0":""))+boost::lexical_cast<std::string>(ltm->tm_mday)+"_"+boost::lexical_cast<std::string>(1 + ltm->tm_mon < 10 ?"0":"")+boost::lexical_cast<std::string>(1 + ltm->tm_mon)+"_"+boost::lexical_cast<std::string>(1900 + ltm->tm_year);
	return date;
}

void DBConnection::insertIntoTableAccTime(RowData *rowData,string acctime)
{
	this->insPstmtAccTime->setString(1,rowData->user);
	this->insPstmtAccTime->setString(2,rowData->domain);
	this->insPstmtAccTime->setString(3,acctime);
    this->insPstmtAccTime->executeUpdate();
}

void DBConnection::insertIntoTableDenTime(RowDataDenied *rowData,string acctime)
{
    this->insPstmtDenTime->setString(1,rowData->user);
    this->insPstmtDenTime->setString(2,rowData->domain);
    this->insPstmtDenTime->setString(3,acctime);
    this->insPstmtDenTime->executeUpdate();
}


void DBConnection::insertIntoTableAcc(RowData *rowData)
{
    this->insPstmtAcc->setString(1,rowData->user);
    this->insPstmtAcc->setString(2,rowData->domain);
    this->insPstmtAcc->setDouble(3,rowData->size);
    this->insPstmtAcc->setInt(4,rowData->connection);
    this->insPstmtAcc->setDouble(5,rowData->hit);
    this->insPstmtAcc->setDouble(6,rowData->miss);
    this->insPstmtAcc->setDouble(7,rowData->respone_time);
    this->insPstmtAcc->executeUpdate();
}

void DBConnection::updateTableAcc(RowData *rowData)
{
    this->upPstmtAcc->setDouble(1,rowData->size);
    this->upPstmtAcc->setInt(2,rowData->connection);
    this->upPstmtAcc->setDouble(3,rowData->hit);
    this->upPstmtAcc->setDouble(4,rowData->miss);
    this->upPstmtAcc->setDouble(5,rowData->respone_time);
    this->upPstmtAcc->setString(6,rowData->user);
    this->upPstmtAcc->setString(7,rowData->domain);

    this->upPstmtAcc->executeUpdate();
    return;
}

void DBConnection::insertIntoTableDen(RowDataDenied *rowData)
{
    this->insPstmtDen->setString(1,rowData->user);
    this->insPstmtDen->setString(2,rowData->domain);
    this->insPstmtDen->setInt(3,rowData->connection);
    this->insPstmtDen->executeUpdate();
    return;
}

void DBConnection::updateTableDen(RowDataDenied *rowData)
{
    this->upPstmtDen->setInt(1,rowData->connection);
    this->upPstmtDen->setString(2,rowData->user);
    this->upPstmtDen->setString(3,rowData->domain);
    this->upPstmtDen->executeUpdate();
    return;
}

void DBConnection::readTable()
{
    this->res = this->readpstmt->executeQuery();
    return;
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

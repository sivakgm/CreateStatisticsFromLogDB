/*
 * DBConnection.cpp

 *
 *  Created on: Nov 5, 2014
 *      Author: sivaprakash
 *
 */

#include "DBConnection.h"
#include "RowData.h"

void DBConnection::dbConnOpen(string host,string port,string user,string pass,string schema)
{
	cout<<"Open Database Connection\n";
	Driver *driver = get_driver_instance();
	string add = "tcp://"+host+":"+port;
	this->conn = driver->connect(add,user,pass);
	this->conn->setSchema(schema);
	return;
}

void DBConnection::createStatTableName()
{
	this->tableNameAcc = "ud_acc_"+timeAndDate();
	this->tableNameDen = "ud_den_"+timeAndDate();
	this->createTableIfNotExist();
	this->setPstmt();
	return;
}

bool DBConnection::createTableIfNotExist()
{
	this->stmt=this->conn->createStatement();
	this->stmt->execute("create table if not exists " + this->tableNameAcc + "(user varchar(12), domain varchar(100), size double, connection int, hit float, miss float);");
	return this->stmt->execute("create table if not exists " + this->tableNameDen + "(user varchar(12), domain varchar(100), connection int);");
}

void DBConnection::setPstmt()
{

	string query = "insert into " + this->tableNameAcc +"(user,domain,size,connection,hit,miss) values(?,?,?,?,?,?)";
	this->insPstmtAcc=this->conn->prepareStatement(query);

	query = "update " + this->tableNameAcc + " set size=?,connection=?,hit=?,miss=? where user=? and domain=?;";
	this->upPstmtAcc=this->conn->prepareStatement(query);


	query = "insert into " + this->tableNameDen +"(user,domain,connection) values(?,?,?)";
	this->insPstmtDen=this->conn->prepareStatement(query);

	query = "update " + this->tableNameDen + " set connection=? where user=? and domain=?;";
	this->upPstmtDen=this->conn->prepareStatement(query);
}

void DBConnection::setReadPstmt(int a,string tableName,string user,string domain)
{
	string query;

	if(a == 1)
	{
		query = "select * from " + tableName +";";
	}
	else
	{
		query = "select * from " + tableName +" where user=? and domain=?;";
	}

	this->readpstmt = this->conn->prepareStatement(query);

	if(a != 1 )
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

void DBConnection::insertIntoTableAcc(RowData *rowData)
{
    this->insPstmtAcc->setString(1,rowData->user);
    this->insPstmtAcc->setString(2,rowData->domain);
    this->insPstmtAcc->setDouble(3,rowData->size);
    this->insPstmtAcc->setInt(4,rowData->connection);
    this->insPstmtAcc->setDouble(5,rowData->hit);
    this->insPstmtAcc->setDouble(6,rowData->miss);
    this->insPstmtAcc->executeUpdate();
}

void DBConnection::updateTableAcc(RowData *rowData)
{
    this->upPstmtAcc->setDouble(1,rowData->size);
    this->upPstmtAcc->setInt(2,rowData->connection);
    this->upPstmtAcc->setDouble(3,rowData->hit);
    this->upPstmtAcc->setDouble(4,rowData->miss);
    this->upPstmtAcc->setString(5,rowData->user);
    this->upPstmtAcc->setString(6,rowData->domain);
    this->upPstmtAcc->executeUpdate();
    return;
}

void DBConnection::insertIntoTableDen(RowData *rowData)
{
    this->insPstmtDen->setString(1,rowData->user);
    this->insPstmtDen->setString(2,rowData->domain);
    this->insPstmtDen->setInt(3,rowData->connection);
    this->insPstmtDen->executeUpdate();
    return;
}

void DBConnection::updateTableDen(RowData *rowData)
{
    this->upPstmtDen->setInt(1,rowData->connection);
    this->upPstmtDen->setString(2,rowData->user);
    this->upPstmtDen->setString(3,rowData->domain);
    this->upPstmtDen->executeUpdate();
    return;
}

void DBConnection::readTable(int a,string tableName,string user,string domain)
{
	this->setReadPstmt(a,tableName,user,domain);
    this->res = this->readpstmt->executeQuery();
    return;
}


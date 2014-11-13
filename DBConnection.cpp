/*
 * DBConnection.cpp
 *
 *  Created on: Nov 5, 2014
 *      Author: sivaprakash
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
	this->tableNameAcc = "ud_acc"+timeAndDate();
	this->tableNameDen = "ud_den"+timeAndDate();
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


	string query = "insert into " + this->tableNameDen +"(user,domain,connection) values(?,?,?)";
	this->insPstmtDen=this->conn->prepareStatement(query);

	query = "update " + this->tableNameDen + " set connection=? where user=? and domain=?;";
	this->upPstmtDen=this->conn->prepareStatement(query);


}

string timeAndDate()
{
	time_t now = time(0);
	tm *ltm = localtime(&now);
	string date = boost::lexical_cast<std::string>((ltm->tm_mday < 10 ?"0":""))+boost::lexical_cast<std::string>(ltm->tm_mday)+"_"+boost::lexical_cast<std::string>(1 + ltm->tm_mon < 10 ?"0":"")+boost::lexical_cast<std::string>(1 + ltm->tm_mon)+"_"+boost::lexical_cast<std::string>(1900 + ltm->tm_year);
	return date;
}

void DBConnection::insertIntoTable(RowData *rowData)
{
    this->insPstmt->setString(1,rowData->user);
    this->insPstmt->setString(2,rowData->domain);
    this->insPstmt->setDouble(3,rowData->size);
    this->insPstmt->setInt(4,rowData->connection);
    this->insPstmt->setDouble(5,rowData->hit);
    this->insPstmt->setDouble(6,rowData->miss);
    this->insPstmt->executeUpdate();
    return;
}

void DBConnection::readTable()
{
    cout<<"Open Database Connection\n";
    string qurey = "select * from " + this->tableName ;
    this->readpstmt = this->conn->prepareStatement(qurey);
    this->res = this->readpstmt->executeQuery();
    return;
}

void DBConnection::updateTable(RowData *rowData)
{
    this->upPstmt->setDouble(1,rowData->size);
    this->upPstmt->setInt(2,rowData->connection);
    this->upPstmt->setDouble(3,rowData->hit);
    this->upPstmt->setDouble(4,rowData->miss);
    this->upPstmt->setString(5,rowData->user);
    this->upPstmt->setString(6,rowData->domain);
    this->upPstmt->executeUpdate();
    return;
}

void DBConnection::executePstmt(PreparedStatement *pstmt)
{

}


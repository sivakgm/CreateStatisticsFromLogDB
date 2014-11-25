/*
 * DomainStatistics.cpp
 *
 *  Created on: Nov 21, 2014
 *      Author: sivaprakash
 */

#include "DomainStatistics.h"
#include "RowData.h"
#include "RowDataDenied.h"

extern DBConnection *statLog;


void createDomainStatisticsAcc(string tableName)
{
	string domainQuery = "select * from "+ tableName + " order by domain;";

	PreparedStatement *readPstmt;
	ResultSet *udStatData;


	string year = tableName.substr(13,4);
	string month = tableName.substr(10,2);
	string day = tableName.substr(7,2);

	string yearStatisticstable = "d_acc_"+year;
	string monthStatisticstable = "d_acc_"+month;
	string dayStatisticstable = "d_acc_"+day+"_"+month+"_"+year;

	Statement *stmt = statLog->conn->createStatement();
	string domain="";
	RowData *rowData = new RowData();

	checkPresenecOfDomainStatisticsTableAcc(stmt,dayStatisticstable);
	checkPresenecOfDomainStatisticsTableAcc(stmt,monthStatisticstable);
	checkPresenecOfDomainStatisticsTableAcc(stmt,yearStatisticstable);

	readPstmt = statLog->conn->prepareStatement(domainQuery);
	udStatData = readPstmt->executeQuery();

	while(udStatData->next())
	{
		if(udStatData->getString(2) == domain)
		{
			rowData->size = rowData->size + udStatData->getDouble(3);
			rowData->connection = rowData->connection + udStatData->getInt(4);
			rowData->hit = rowData->hit + udStatData->getDouble(5);
			rowData->miss = rowData->miss + udStatData->getDouble(6);
			rowData->response_time = rowData->response_time + udStatData->getDouble(7);
		}
		else
		{
			if(domain != "")
			{
				insertDataIntoDailyDomainStatisticsAcc(rowData,stmt,dayStatisticstable);
				checkPresenceOfDomainDataInTableAcc(rowData,stmt,monthStatisticstable);
				checkPresenceOfDomainDataInTableAcc(rowData,stmt,yearStatisticstable);
			}
			domain = udStatData->getString(2);
			rowData->domain = udStatData->getString(2);
			rowData->size = udStatData->getDouble(3);
			rowData->connection = udStatData->getInt(4);
			rowData->hit = udStatData->getDouble(5);
			rowData->miss = udStatData->getDouble(6);
			rowData->response_time = udStatData->getDouble(7);
		}

	}
	insertDataIntoDailyDomainStatisticsAcc(rowData,stmt,dayStatisticstable);
	checkPresenceOfDomainDataInTableAcc(rowData,stmt,monthStatisticstable);
	checkPresenceOfDomainDataInTableAcc(rowData,stmt,yearStatisticstable);
}

void insertDataIntoDailyDomainStatisticsAcc(RowData *rowData,Statement *stmt,string tableName)
{
	stmt->execute("insert into "+ tableName +"(domain,size,connection,hit,miss,response_time) values('"+ rowData->domain +"'," + boost::lexical_cast<std::string>(rowData->size) + "," + boost::lexical_cast<std::string>(rowData->connection) + "," + boost::lexical_cast<std::string>(rowData->hit) + "," + boost::lexical_cast<std::string>(rowData->miss) + "," + boost::lexical_cast<std::string>(rowData->response_time) + ");");
}

void checkPresenceOfDomainDataInTableAcc(RowData *rowData,Statement *stmt,string tableName)
{
	ResultSet *res = stmt->executeQuery("select * from "+tableName+" where domain='"+rowData->domain+"';");
	if(res->next())
	{
		rowData->size = rowData->size + res->getDouble(2);
		rowData->connection = rowData->connection + res->getInt(3);
		rowData->hit = rowData->hit + res->getDouble(4);
		rowData->miss = rowData->miss + res->getDouble(5);
		rowData->response_time = rowData->response_time + res->getDouble(6);
		stmt->executeUpdate("update "+tableName+" set size="+boost::lexical_cast<std::string>(rowData->size)+",connection=" + boost::lexical_cast<std::string>(rowData->connection) + ",hit="+ boost::lexical_cast<std::string>(rowData->hit) + ",miss=" + boost::lexical_cast<std::string>(rowData->miss) + ",response_time=" + boost::lexical_cast<std::string>(rowData->response_time) + " where domain='" + rowData->domain + "';");
	}
	else
	{
		insertDataIntoDailyDomainStatisticsAcc(rowData,stmt,tableName);
	}
}

void checkPresenecOfDomainStatisticsTableAcc(Statement *stmt,string tableName)
{
	stmt->execute("create table if not exists " + tableName + "(domain varchar(100), size double, connection int, hit float, miss float,response_time float);");
}

void createDomainStatisticsDen(string tableName)
{
	string domainQuery = "select * from "+ tableName + " order by domain;";

	PreparedStatement *readPstmt;
	ResultSet *udStatData;


	string year = tableName.substr(13,4);
	string month = tableName.substr(10,2);
	string day = tableName.substr(7,2);

	string yearStatisticstable = "d_den_"+year;
	string monthStatisticstable = "d_den_"+month;
	string dayStatisticstable = "d_den_"+day+"_"+month+"_"+year;

	Statement *stmt = statLog->conn->createStatement();
	string domain="";
	RowDataDenied *rowDataDenied = new RowDataDenied();

	checkPresenecOfDomainStatisticsTableDen(stmt,dayStatisticstable);
	checkPresenecOfDomainStatisticsTableDen(stmt,monthStatisticstable);
	checkPresenecOfDomainStatisticsTableDen(stmt,yearStatisticstable);

	readPstmt = statLog->conn->prepareStatement(domainQuery);
	udStatData = readPstmt->executeQuery();

	while(udStatData->next())
	{
		if(udStatData->getString(2) == domain)
		{
			rowDataDenied->connection = rowDataDenied->connection + udStatData->getInt(3);
		}
		else
		{
			if(domain != "")
			{
				insertDataIntoDailyDomainStatisticsDen(rowDataDenied,stmt,dayStatisticstable);
				checkPresenceOfDomainDataInTableDen(rowDataDenied,stmt,monthStatisticstable);
				checkPresenceOfDomainDataInTableDen(rowDataDenied,stmt,yearStatisticstable);
			}
			domain = udStatData->getString(2);
			rowDataDenied->domain = udStatData->getString(2);
			rowDataDenied->connection = udStatData->getInt(3);
		}

	}
	insertDataIntoDailyDomainStatisticsDen(rowDataDenied,stmt,dayStatisticstable);
	checkPresenceOfDomainDataInTableDen(rowDataDenied,stmt,monthStatisticstable);
	checkPresenceOfDomainDataInTableDen(rowDataDenied,stmt,yearStatisticstable);
}

void insertDataIntoDailyDomainStatisticsDen(RowDataDenied *rowDataDenied,Statement *stmt,string tableName)
{
	stmt->execute("insert into "+ tableName +"(domain,connection) values('"+rowDataDenied->domain+"'," + boost::lexical_cast<std::string>(rowDataDenied->connection) +");");
}

void checkPresenceOfDomainDataInTableDen(RowDataDenied *rowDataDenied,Statement *stmt,string tableName)
{
	ResultSet *res = stmt->executeQuery("select * from "+tableName+" where domain='"+rowDataDenied->domain+"';");
	if(res->next())
	{
		rowDataDenied->connection = rowDataDenied->connection + res->getInt(2);
		stmt->executeUpdate("update "+tableName+" set connection="+boost::lexical_cast<std::string>(rowDataDenied->connection)+" where domain='"+rowDataDenied->domain+"' ;");
	}
	else
	{
		insertDataIntoDailyDomainStatisticsDen(rowDataDenied,stmt,tableName);
	}
}

void checkPresenecOfDomainStatisticsTableDen(Statement *stmt,string tableName)
{
	stmt->execute("create table if not exists " + tableName + "(domain varchar(100), connection int);");
}

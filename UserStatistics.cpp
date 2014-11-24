/*
 * UserStatistics.cpp
 *
 *  Created on: Nov 21, 2014
 *      Author: sivaprakash
 */

#include "UserStatistics.h"

extern DBConnection *statLog;


void createUserStatistics(string tableName)
{
	string disUserQuery = "select distinct user from "+ tableName + ";";
	string userAccessQuery = "select * from " + tableName + " where user=? ;";

	PreparedStatement *readPstmt,*inPstmt,*upPstmt;
	ResultSet *userList,*userData;

	readPstmt = statLog->conn->prepareStatement(disUserQuery);
	userList = readPstmt->executeQuery();

	readPstmt = statLog->conn->prepareStatement(userAccessQuery);

	while(userList->next())
	{
		string user = userList->getString(1);
		readPstmt->setString(1,user);
		userData = readPstmt->executeQuery();

		while(userData->next())
		{

		}
	}

}


/*namespace boost {

UserStatistics::UserStatistics() {
	// TODO Auto-generated constructor stub

}

UserStatistics::~UserStatistics() {
	// TODO Auto-generated destructor stub
}

} /* namespace boost */



/*
 * grossStatistics.h
 *
 *  Created on: Nov 20, 2014
 *      Author: sivaprakash
 */

#ifndef GROSSSTATISTICS_H_
#define GROSSSTATISTICS_H_

void grossStatisticsAcc(string tableName);
void insertRowDataAcc(ResultSet *dRes,PreparedStatement *pstmt);
void updateRowDataAcc(ResultSet *dRes,ResultSet *ymRes,PreparedStatement *pstmt);
void grossStatisticsDen(string tableName);
void insertRowDataDen(ResultSet *dRes,PreparedStatement *pstmt);
void updateRowDataDen(ResultSet *dRes,ResultSet *ymRes,PreparedStatement *pstmt);


#endif /* GROSSSTATISTICS_H_ */

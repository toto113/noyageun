/************************************************************************************************************	
Package Name:	com.kt.bit.csm.blds.utility
Author:			Pushpendra Pandey
Description:	
This package contains utility classes like logging and Data access framework of CSM 

Modification Log:	
When                           Version   			Who					 What	
21-12-2010                     1.0                  Pushpendra Pandey    New class created
----------------------------------------------------------------------------------------------------------	
***************************************************************************************************************/
package com.kt.bit.csm.blds.utility;

import oracle.jdbc.OracleCallableStatement;
import oracle.jdbc.OracleResultSet;

import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;

/**************************************************************************
CSMResultSet
============
This class implements utility methods to iterate database result set.
**************************************************************************/
public class CSMResultSet {
    public static int RESULT_MODE_CACHE = 1;
    public static int RESULT_MODE_ORACLE = 2;

	private OracleCallableStatement	cs	= null;
	private OracleResultSet	        rs	= null;
    private CachedResultSet         crs = null;
    private int                     dataSourceType = CSMResultSet.RESULT_MODE_ORACLE;

    public int getDataSourceType() {
        return dataSourceType;
    }

    public void setDataSourceType(int dataSourceType) {
        this.dataSourceType = dataSourceType;
    }



    public CSMResultSet(CachedResultSet cachedResultSet){

        this.crs = cachedResultSet;
    }

	/**************************************************************************
	CSMResultSet
	============
	Parameterized constructor to construct CSMResultSet object 
		
	@param deptResultSet - OracleResultSet object
	@param cs - OracleCallableStatement object
	**************************************************************************/
	public CSMResultSet(OracleResultSet deptResultSet, OracleCallableStatement cs) {
		this.rs = deptResultSet;
		this.cs = cs;
	}

    public CachedResultSet getCrs() {
        return crs;
    }

    public void setCachedResultSet(CachedResultSet crs) {
        this.crs = crs;
    }

	/**************************************************************************
	close
	=====
	This method used to close connection with database
		
	@exception java.sql.SQLException - Database exception
	**************************************************************************/
	public void close() throws SQLException {
        if( dataSourceType == CSMResultSet.RESULT_MODE_ORACLE){
		    rs.close();
		    cs.close();
		    rs = null;
		    cs = null;
        }
	}

	/**************************************************************************
	next
	====
	This method used to return next row from database result set

	@return boolean
	@exception java.sql.SQLException - Database exception
	**************************************************************************/
	public boolean next() throws SQLException {
        if( dataSourceType == CSMResultSet.RESULT_MODE_CACHE){
            return crs.next();
        }
        else{
            return rs.next();
        }
	}

	/**************************************************************************
	getInt
	======
	This method used to get value of NUMBER type column from result set

	@param i - CacheColumn number
	@return int
	@exception java.sql.SQLException - Database exception
	**************************************************************************/
	public int getInt(int i) throws SQLException, Exception {
        if( dataSourceType == CSMResultSet.RESULT_MODE_CACHE){
            return crs.getInt(i);
        }
        else{
            return rs.getInt(i);
        }

	}

	/**************************************************************************
	getString
	=========
	This method used to get row value of column type VARCHAR from result set

	@param
	@return String
	@exception java.sql.SQLException - Database exception
	**************************************************************************/
	public String getString(int i) throws SQLException, Exception {
        if( dataSourceType == CSMResultSet.RESULT_MODE_CACHE){
            return crs.getString(i);
        }
        else{
            return rs.getString(i);
        }
	}

	/**************************************************************************
	getLong
	=======
	This method used to get value of LONG type column from result set

	@param i - CacheColumn number
	@return long
	@exception java.sql.SQLException - Database exception
	**************************************************************************/
	public long getLong(int i) throws SQLException, Exception {
        if( dataSourceType == CSMResultSet.RESULT_MODE_CACHE){
            return crs.getLong(i);
        }
        else{
            return rs.getLong(i);
        }

	}

	/**************************************************************************
	getLong
	=======
	This method used to get value of LONG type column from result set

	@param  string
	@return long
	@exception java.sql.SQLException - Database exception
	**************************************************************************/
	public long getLong(String string) throws SQLException, Exception {
        if( dataSourceType == CSMResultSet.RESULT_MODE_CACHE){
            return crs.getLong(string);
        }
        else{
            return rs.getLong(string);
        }

	}

	/**************************************************************************
	getString
	=========
	This method used to get row value of column type VARCHAR from  result set

	@param string - CacheColumn name
	@return String
	@exception java.sql.SQLException - Database exception
	**************************************************************************/
	public String getString(String string) throws SQLException, Exception {

        if( dataSourceType == CSMResultSet.RESULT_MODE_CACHE){
            return crs.getString(string);
        }
        else{
            return rs.getString(string);
        }
	}

	/**************************************************************************
	getInt
	======
	This method used to get value of NUMBER type column from result set

	@param string - CacheColumn name
	@return int
	@exception java.sql.SQLException - Database exception
	**************************************************************************/
	public int getInt(String string) throws SQLException, Exception {
        if( dataSourceType == CSMResultSet.RESULT_MODE_CACHE){
            return crs.getInt(string);
        }
        else{
            return rs.getInt(string);
        }
	}

	/**************************************************************************
	getDate
	=======
	This method used to get Date from Date column in database result set

	@param string - CacheColumn name
	@return Date
	@exception java.sql.SQLException - Database exception
	**************************************************************************/
	public Date getDate(String string) throws SQLException, Exception {
        if( dataSourceType == CSMResultSet.RESULT_MODE_CACHE){
            return crs.getDate(string);
        }
        else{
            return rs.getDate(string);
        }
	}

	/**************************************************************************
	getDate
	=======
	This method used to get Date from Date column in database result set

	@param i - CacheColumn number
	@return Date
	@exception java.sql.SQLException - Database exception
	**************************************************************************/
	public Date getDate(int i) throws SQLException, Exception {
        if( dataSourceType == CSMResultSet.RESULT_MODE_CACHE){
            return crs.getDate(i);
        }
        else{
            return rs.getDate(i);
        }
	}

	/**************************************************************************
	getTimestamp
	=======
	This method used to get Timestamp from Date column in database result set

	@param string - CacheColumn name
	@return Timestamp
	@exception java.sql.SQLException - Database exception
	**************************************************************************/
	public Timestamp getTimestamp(String string) throws SQLException, Exception {
        if( dataSourceType == CSMResultSet.RESULT_MODE_CACHE){
            return crs.getTimestamp(string);
        }
        else{
            return rs.getTimestamp(string);
        }
	}

	/**************************************************************************
	getTimestamp
	=======
	This method used to get Timestamp from Date column in database result set

	@param i - CacheColumn number
	@return Timestamp
	@exception java.sql.SQLException - Database exception
	**************************************************************************/
	public Timestamp getTimestamp(int i) throws SQLException, Exception {
        if( dataSourceType == CSMResultSet.RESULT_MODE_CACHE){
            return crs.getTimestamp(i);
        }
        else{
            return rs.getTimestamp(i);
        }
	}


    public Object getObject(int i) throws SQLException, Exception{
        if( dataSourceType == CSMResultSet.RESULT_MODE_CACHE){
            return crs.getObject(i);
        }
        else{
            return rs.getObject(i);
        }
    }


    public Object getObject(String string) throws SQLException, Exception{
        if( dataSourceType == CSMResultSet.RESULT_MODE_CACHE){
            return crs.getObject(string);
        }
        else{
            return rs.getObject(string);
        }
    }

	/**************************************************************************
	getRow
	======
	This method used to get current row number from result set

	@return int
	@exception java.sql.SQLException - Database exception
	**************************************************************************/
	public int getRow() throws SQLException {
		return rs.getRow();
	}

	/**************************************************************************
	last
	====
	This method used to return last from database result set

	@return boolean
	@exception java.sql.SQLException - Database exception
	**************************************************************************/
	public boolean last() throws SQLException {
		return rs.last();
	}

	/**************************************************************************
	getBytes
	========
	This method used to get byte data

	@param string - CacheColumn name
	@return byte[]
	@exception java.sql.SQLException - Database exception
	**************************************************************************/
	public byte[] getBytes(String string) throws SQLException {
		return rs.getBytes(string);
	}

	public ResultSetMetaData getMetaData() throws SQLException {
		return rs.getMetaData();
	}

}

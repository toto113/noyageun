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

import com.kt.bit.csm.blds.cache.CSMCacheManager;
import oracle.jdbc.OracleCallableStatement;
import oracle.jdbc.OracleResultSet;
import oracle.jdbc.OracleTypes;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**************************************************************************
 * This class exposes capabilities for executing stored procedures against the
 * database, by means of the Reusable Architecture Framework Data Access Module.
 * 
 **************************************************************************/
public class DataAccessManager {
	private final DBAccessor dbAccessor;
	private Connection conn = null;
    private CSMCacheManager cacheManager;


	private static Map<String, String> autocommitProcMap = new HashMap<String, String>();

	/**
	 * Default constructor.
	 * 
	 * @throws Exception
	 *             the exception
	 */
	public DataAccessManager() throws Exception {
	    /*
		if ( autocommitProcMap.isEmpty() ) {
			String[] autocommitProcKeys = ConfigUtil.getStringArray("CSM.AUTOCOMMIT.PROCNAMES");
			if( autocommitProcKeys != null ) {
    			for (String key : autocommitProcKeys) {
    				autocommitProcMap.put(key, "true");
    			}
			}
		}
		*/
		this.dbAccessor = DBAccessor.getInstance();
        this.cacheManager = CSMCacheManager.getInstance();
	}

	/**
	 * This method takes the request Generate the SQL Command.
	 * 
	 * @param spName
	 *            -SP Name
	 * @param paramNumber
	 *            - Param number
	 * @return sqlCommand - Generated SQL command
	 */
	private String buildString(String spName, int paramNumber) {
	    StringBuffer sql = new StringBuffer( "{call " ).append( spName ).append( "(?" );
		for (int i = 0; i < paramNumber; i++) {
		    sql.append( ",?" );
		}
		sql.append( ")}" );
		return sql.toString();
	}

	/**
	 * This method takes in the request to execute a stored procedure against
	 * the SDP Database to execute queries that return a String as result.
	 * 
	 * @param conn
	 *            - Connection object
	 * @param spName
	 *            - StoredProcedure name
	 * @param params
	 *            the params
	 * @return params DAMParam
	 * @throws java.sql.SQLException
	 *             the sQL exception
	 */
	private OracleCallableStatement prepareCallableStatement(Connection conn, String spName, DAMParam[] params) throws SQLException {
		String sqlCommand = buildString(spName, params.length);
		OracleCallableStatement cs = (OracleCallableStatement) conn.prepareCall(sqlCommand);
		for (int i = 0; i < params.length; i++) {
			cs.setObject(params[i].getParamName(), params[i].getValue(), params[i].getType());
		}
		return cs;
	}

    public CSMResultSet executeStoredProcedureForQuery(String spName, String outParamName, DAMParam[] param) throws Exception {
        OracleCallableStatement oraCallStmt = null;
        OracleResultSet deptResultSet = null;
        CSMResultSet csmResultSet = null;
        boolean isCacheTarget = false;
        boolean existInCache = false;
        StringBuffer key = null;

        /**
         *  cache 에서 Data 를 조회하는 구간
         *  1. Cache  대상인지 확인
         *  2. 대상인 경우 Cache 에 존재하는지 확인 ( Key = spName + param.getValue()[] )
         *  3. Cache 에 존재하는 경우 Cache 에서 조회하여 CSMResultSet 구성하여 return
         *  4. Cache 에 존재하지 않는 경우 기존 로직 수행
         *  5. Cache 대상이 아닌 경우도 기존 로직 수행
         */
        isCacheTarget = this.cacheManager.isCacheTarget(spName);

        if(isCacheTarget){
            key = new StringBuffer();
            key.append(spName);
            for(int i = 0; i < param.length; i++){
                key.append(param[i].getValue()).append("|");
            }

            csmResultSet = this.cacheManager.getResultSet(key.toString());


        }

        if( csmResultSet == null ){
            if (conn == null || conn.isClosed()) {
                conn = dbAccessor.getConnection();
            }
            // Prepare the callable statement
            oraCallStmt = prepareCallableStatement(conn, spName, param);
            oraCallStmt.registerOutParameter(outParamName, OracleTypes.CURSOR);
            oraCallStmt.execute();

            deptResultSet = (OracleResultSet) oraCallStmt.getObject(outParamName);
            csmResultSet = new CSMResultSet(deptResultSet, oraCallStmt);

            /**
             *  cache 에서 Data 를 조회하는 구간
             *  1. Cache  대상인지 확인
             *  2. Cache 대상인 경우 Cache 에 저장( 비동기 처리검토가 필요 )
             *  3. csmResultSet return.
             */
            if(isCacheTarget && key != null){
                this.cacheManager.putResultSet(key.toString(), csmResultSet);
            }
        }

        return csmResultSet;
    }
//
//	/**
//	 * This method takes in the request to execute a stored procedure against
//	 * the SDP Database to execute queries that return a Boolean as result.
//	 *
//	 * @param spName
//	 *            - String stored procedure name
//	 * @param outParamName
//	 *            - String output parameter name of the called stored procedure
//	 * @return boolean - value resulting from the stored procedure execution
//	 * @throws Exception
//	 *             the exception
//	 */
//	public boolean executeStoredProcedureForBoolean(String spName, String outParamName) throws Exception {
//		DAMParam[] param = {};
//		return executeStoredProcedureForBoolean(spName, outParamName, param);
//	}
//
//	/**
//	 * This method takes in the request to execute a stored procedure against
//	 * the SDP Database to execute queries that return a Boolean as result.
//	 *
//	 * @param spName
//	 *            - String stored procedure name
//	 * @param outParamName
//	 *            - String output parameter name of the called stored procedure
//	 * @param param
//	 *            - DAMParam array with input parameters to call the stored
//	 *            procedure
//	 * @return boolean - value resulting from the stored procedure execution
//	 * @throws Exception
//	 *             the exception
//	 */
//	public boolean executeStoredProcedureForBoolean(String spName, String outParamName, DAMParam[] param) throws Exception {
//		OracleCallableStatement oraCallStmt = null;
//
//		try {
//			if (this.conn == null || this.conn.isClosed()) {
//				this.conn = this.dbAccessor.getConnection();
//			}
//
//			oraCallStmt = prepareCallableStatement(this.conn, spName, param);
//			oraCallStmt.registerOutParameter(outParamName, OracleTypes.NUMBER);
//			oraCallStmt.execute();
//
//			int intResult = oraCallStmt.getInt(outParamName);
//			oraCallStmt.close();
//
//			commenseCommitOnAutoCommitProcs(spName);
//			if (intResult == 0) {
//				return false;
//			} else if (intResult == 1) {
//				return true;
//			}
//
//			if (SdpCommonLogUtil.isErrorEnabled()) {
//				SdpCommonLogUtil
//						.writeErrorLog(Constants.EXCEPTION_ERROR_CODE, "The stored procedure executed returned a value different from 0 or 1");
//			}
//
//			throw new Exception("The stored procedure executed returned a value different from 0 or 1");
//		} catch (Exception e) {
//		    if (SdpCommonLogUtil.isErrorEnabled()) {
//                SdpCommonLogUtil.writeErrorLog(Constants.EXCEPTION_ERROR_CODE,e.getMessage(), e );
//            }
//			throw e;
//		} finally {
//			if (oraCallStmt != null) {
//				try {
//					oraCallStmt.close();
//					oraCallStmt = null;
//				} catch (Exception e) {
//				    if (SdpCommonLogUtil.isErrorEnabled()) {
//                        SdpCommonLogUtil.writeErrorLog(Constants.EXCEPTION_ERROR_CODE,e.getMessage(), e );
//                    }
//				}
//			}
//		}
//	}
//
//	/**
//	 * This method takes in the request to execute a stored procedure against
//	 * the SDP Database to execute queries that return a Boolean as result.
//	 *
//	 * @param spName
//	 *            - String stored procedure name
//	 * @param outParamName
//	 *            - String output parameter name of the called stored procedure
//	 * @return CSMResultSet resulSet object resulting from the stored procedure
//	 *         execution
//	 * @throws Exception
//	 *             the exception
//	 */
//	public CSMResultSet executeStoredProcedureForQuery(String spName, String outParamName) throws Exception {
//		DAMParam[] param = {};
//		return executeStoredProcedureForQuery(spName, outParamName, param);
//	}
//
//
//	/**
//	 * This method takes in the request to execute a stored procedure
//	 * the SDP Database to execute queries that return a Boolean as result.
//	 *
//	 * @param spName
//	 *            - String stored procedure name
//	 * @param outParamName
//	 *            - String output parameter name of the called stored procedure
//	 * @param param
//	 *            - DAMParam array with input parameters to call the stored
//	 *            procedure
//	 * @return CSMResultSet resulSet object resulting from the stored procedure
//	 *         execution
//	 * @throws Exception
//	 *             the exception
//	 */
//	public CSMResultSet executeStoredProcedureForQuery(String spName, String outParamName, DAMParam[] param) throws Exception {
//		OracleCallableStatement oraCallStmt = null;
//		OracleResultSet deptResultSet = null;
//		CSMResultSet csmResultSet = null;
//        boolean isCache = false;
//        boolean existInCache = false;
//
//        /**
//         *  cache 에서 Data 를 조회하는 구간
//         *  1. Cache  대상인지 확인
//         *  2. 대상인 경우 Cache 에 존재하는지 확인 ( Key = spName + param.getValue()[] )
//         *  3. Cache 에 존재하는 경우 Cache 에서 조회하여 CSMResultSet 구성하여 return
//         *  4. Cache 에 존재하지 않는 경우 기존 로직 수행
//         *  5. Cache 대상이 아닌 경우도 기존 로직 수행
//         */
//        isCache =
//
//
//
//
//        if (conn == null || conn.isClosed()) {
//			conn = dbAccessor.getConnection();
//		}
//		// Prepare the callable statement
//		oraCallStmt = prepareCallableStatement(conn, spName, param);
//		oraCallStmt.registerOutParameter(outParamName, OracleTypes.CURSOR);
//		oraCallStmt.execute();
//
//		deptResultSet = (OracleResultSet) oraCallStmt.getObject(outParamName);
//		csmResultSet = new CSMResultSet(deptResultSet, oraCallStmt);
//
//        /**
//         *  cache 에서 Data 를 조회하는 구간
//         *  1. Cache  대상인지 확인
//         *  2. Cache 대상인 경우 Cache 에 저장( 비동기 처리검토가 필요 )
//         *  3. csmResultSet return.
//         */
//
//
//		return csmResultSet;
//	}
//
//	/**
//	 * This method takes in the request to execute a stored procedure against
//	 * the SDP Database to execute queries that return a scalar as result.
//	 *
//	 * @param spName
//	 *            - String stored procedure name
//	 * @param outParamName
//	 *            - String output parameter name of the called stored procedure
//	 * @return int scalar result from the stored procedure execution
//	 * @throws Exception
//	 *             the exception
//	 */
//	public int executeStoredProcedureForScalar(String spName, String outParamName) throws Exception {
//		DAMParam[] dummyParam = {};
//		return executeStoredProcedureForScalar(spName, outParamName, dummyParam);
//	}
//
//	/**
//	 * This method takes in the request to execute a stored procedure against
//	 * the SDP Database to execute queries that return a scalar as result.
//	 *
//	 * @param spName
//	 *            - String stored procedure name
//	 * @param outParamName
//	 *            - String output parameter name of the called stored procedure
//	 * @param param
//	 *            - DAMParam array with input parameters to call the stored
//	 *            procedure
//	 * @return int result from the stored procedure execution
//	 * @throws Exception
//	 *             the exception
//	 */
//	public int executeStoredProcedureForScalar(String spName, String outParamName, DAMParam[] param) throws Exception {
//
//		OracleCallableStatement oraCallStmt = null;
//		int result = 0;
//		try {
//			if (this.conn == null || this.conn.isClosed()) {
//				this.conn = this.dbAccessor.getConnection();
//			}
//
//			oraCallStmt = prepareCallableStatement(this.conn, spName, param);
//			oraCallStmt.registerOutParameter(outParamName, OracleTypes.NUMBER);
//			oraCallStmt.execute();
//
//			result = oraCallStmt.getInt(outParamName);
//			oraCallStmt.close();
//
//			commenseCommitOnAutoCommitProcs(spName);
//
//		} catch (Exception e) {
//		    if (SdpCommonLogUtil.isErrorEnabled()) {
//                SdpCommonLogUtil.writeErrorLog(Constants.EXCEPTION_ERROR_CODE,e.getMessage(), e );
//            }
//			throw e;
//		} finally {
//			if (oraCallStmt != null) {
//				try {
//					oraCallStmt.close();
//					oraCallStmt = null;
//				} catch (Exception e) {
//				    if (SdpCommonLogUtil.isErrorEnabled()) {
//                        SdpCommonLogUtil.writeErrorLog(Constants.EXCEPTION_ERROR_CODE,e.getMessage(), e );
//                    }
//				}
//			}
//		}
//		return result;
//	}
//
//	/**
//	 * This method takes in the request to execute a stored procedure against
//	 * the SDP Database to execute queries that return a scalar as result.
//	 *
//	 * @param spName
//	 *            - String stored procedure name
//	 * @param outParamName
//	 *            - String output parameter name of the called stored procedure
//	 * @return long - result from the stored procedure execution
//	 * @throws Exception
//	 *             the exception
//	 */
//	public long executeStoredProcedureForLongScalar(String spName, String outParamName) throws Exception {
//		DAMParam[] dummyParam = {};
//		return executeStoredProcedureForLongScalar(spName, outParamName, dummyParam);
//	}
//
//	/**
//	 * This method takes in the request to execute a stored procedure against
//	 * the SDP Database to execute queries that return a scalar as result.
//	 *
//	 * @param spName
//	 *            - String stored procedure name
//	 * @param outParamName
//	 *            - String output parameter name of the called stored procedure
//	 * @param param
//	 *            - DAMParam array with input parameters to call the stored
//	 *            procedure
//	 * @return long - result from the stored procedure execution
//	 * @throws Exception
//	 *             the exception
//	 */
//	public long executeStoredProcedureForLongScalar(String spName, String outParamName, DAMParam[] param) throws Exception {
//
//		OracleCallableStatement oraCallStmt = null;
//		long result = 0;
//		try {
//			if (this.conn == null || this.conn.isClosed()) {
//				this.conn = this.dbAccessor.getConnection();
//			}
//
//			oraCallStmt = prepareCallableStatement(this.conn, spName, param);
//			oraCallStmt.registerOutParameter(outParamName, OracleTypes.NUMBER);
//			oraCallStmt.execute();
//
//			result = oraCallStmt.getLong(outParamName);
//			oraCallStmt.close();
//
//			commenseCommitOnAutoCommitProcs(spName);
//		} catch (Exception e) {
//		    if (SdpCommonLogUtil.isErrorEnabled()) {
//                SdpCommonLogUtil.writeErrorLog(Constants.EXCEPTION_ERROR_CODE,e.getMessage(), e );
//            }
//			throw e;
//		} finally {
//			if (oraCallStmt != null) {
//				try {
//					oraCallStmt.close();
//					oraCallStmt = null;
//				} catch (Exception e) {
//				    if (SdpCommonLogUtil.isErrorEnabled()) {
//                        SdpCommonLogUtil.writeErrorLog(Constants.EXCEPTION_ERROR_CODE,e.getMessage(), e );
//                    }
//				}
//			}
//		}
//		return result;
//	}
//
//	/**
//	 * This method takes in the request to execute a stored procedure against
//	 * the SDP Database to execute queries that return a String as result.
//	 *
//	 * @param spName
//	 *            - String stored procedure name
//	 * @param outParamName
//	 *            - String output parameter name of the called stored procedure
//	 * @return string result from the stored procedure execution
//	 * @throws Exception
//	 *             the exception
//	 */
//	public String executeStoredProcedureForString(String spName, String outParamName) throws Exception {
//		DAMParam[] dummyParam = {};
//		return executeStoredProcedureForString(spName, outParamName, dummyParam);
//	}
//
//	/**
//	 * This method takes in the request to execute a stored procedure against
//	 * the SDP Database to execute queries that return a String as result.
//	 *
//	 * @param spName
//	 *            - String stored procedure name
//	 * @param outParamName
//	 *            - String output parameter name of the called stored procedure
//	 * @param param
//	 *            - DAM parameter array
//	 * @return string result from the stored procedure execution
//	 * @throws Exception
//	 *             the exception
//	 */
//	public String executeStoredProcedureForString(String spName, String outParamName, DAMParam[] param) throws Exception {
//		OracleCallableStatement oraCallStmt = null;
//		String result = null;
//		try {
//			if (this.conn == null || this.conn.isClosed()) {
//				this.conn = this.dbAccessor.getConnection();
//			}
//
//			oraCallStmt = prepareCallableStatement(this.conn, spName, param);
//			oraCallStmt.registerOutParameter(outParamName, OracleTypes.VARCHAR);
//			oraCallStmt.execute();
//
//			result = oraCallStmt.getString(outParamName);
//			oraCallStmt.close();
//
//			commenseCommitOnAutoCommitProcs(spName);
//
//		} catch (Exception e) {
//		    if (SdpCommonLogUtil.isErrorEnabled()) {
//                SdpCommonLogUtil.writeErrorLog(Constants.EXCEPTION_ERROR_CODE,e.getMessage(), e );
//            }
//			throw e;
//		} finally {
//			if (oraCallStmt != null) {
//				try {
//					oraCallStmt.close();
//					oraCallStmt = null;
//				} catch (Exception e) {
//				    if (SdpCommonLogUtil.isErrorEnabled()) {
//                        SdpCommonLogUtil.writeErrorLog(Constants.EXCEPTION_ERROR_CODE,e.getMessage(), e );
//                    }
//				}
//			}
//		}
//		return result;
//
//	}
//
//	/**
//	 * This method is responsible of commit and close connection. Is must be
//	 * called after executing a query and before executing again more queries.
//	 *
//	 * @return boolean
//	 * @throws java.sql.SQLException
//	 *             the sQL exception
//	 */
//	public boolean commit() throws SQLException {
//		if (this.conn != null) {
//			DBAccessor.commit(this.conn);
//			return true;
//		} else {
//			return false;
//		}
//	}
//
//    private void commenseCommitOnAutoCommitProcs(String procName) {
//		try {
//			if (autocommitProcMap.containsKey(procName)) {
//				if (SdpCommonLogUtil.isDebugEnabled()) {
//					SdpCommonLogUtil.writeDebugLog("commenseCommitOnAutoCommitProcs invoked for " + procName);
//				}
//
//				commit();
//			}
//		} catch (Exception e) {
//			if (SdpCommonLogUtil.isErrorEnabled()) {
//				SdpCommonLogUtil.writeErrorLog( Constants.EXCEPTION_ERROR_CODE, e.getMessage(), e);
//			}
//			rollBack();
//		} finally {
//			if (ConfigUtil.getBoolean("CSM.AUTOCOMMIT.COMMENSE_CLOSE")) {
//				close();
//			}
//		}
//	}
//
//	/**
//	 * This method is responsible of close connection. Is must be called after
//	 * executing a query and before executing again more queries.
//	 *
//	 * @return boolean
//	 */
//	public boolean close() {
//		try {
//			if (this.conn != null && !this.conn.isClosed()) {
//				DBAccessor.closeConnection(this.conn);
//				return true;
//			} else {
//				return false;
//			}
//		} catch (SQLException e) {
//			if (SdpCommonLogUtil.isErrorEnabled()) {
//				SdpCommonLogUtil.writeErrorLog(Integer.toString(e.getErrorCode()), e.getMessage(), e);
//			}
//		}
//		return false;
//	}
//
//	/**
//	 * This method is responsible of rolling back the query and close the
//	 * connection. Is must be called after executing a query and before
//	 * executing again more queries.
//	 *
//	 * @return boolean true if the roll back and close connection executed
//	 *         correctly. Else false
//	 */
//	public boolean rollBack() {
//		try {
//			if (this.conn != null && !this.conn.isClosed()) {
//				DBAccessor.rollBack(this.conn);
//				return true;
//			} else {
//				return false;
//			}
//		} catch (SQLException e) {
//			if (SdpCommonLogUtil.isErrorEnabled()) {
//				SdpCommonLogUtil.writeErrorLog(Integer.toString(e.getErrorCode()), e.getMessage(), e);
//			}
//		}
//		return false;
//	}
}


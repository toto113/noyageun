package com.kt.bit.csm.blds.common;

import com.kt.bit.csm.blds.utility.*;
import oracle.jdbc.OracleTypes;

import java.sql.SQLException;

/**
  * APIDataAccess
  * package Name : com.kt.bit.csm.APIDataAccess
  * @author  : gye-hwa.chae
  * description  :  API관련 SP정의
  * history
  * --------------------------------------------------
  * when            who                what
  * ---------------------------------------------------
  * 2013. 6. 27.    gye-hwa.chae       신규 Class 생성
  */
public class APIDataAccess {

    private DataAccessManager   dam;
    private String              className;
    private String              transactionID;
    private long                outgoingSequenceNo;
    private StringBuilder       sb = new StringBuilder();

    public static void main(String[] args){
        APIDataAccess ac = new APIDataAccess();
        ac.testCachedResult(args[0], args[1]);
    }


    public APIDataAccess() {
        if (this.dam == null) {
            try {
                this.dam = new DataAccessManager();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        this.className = APIDataAccess.class.toString();
    }



    /**************************************************************************
    loadSubscriptionInfoBySubscriptionID
    ================================
    This capability takes in the request to retrieve Subscription entity.

    @return void
    @exception SDPException - Exception wrapped inside SDPException object
    **************************************************************************/
    public void testCachedResult(String year, String noOfEmployee) {

        /* DebugLog */
//        SdpCommonLogUtil.addPayLoad("Input: Subscription ID=", String.valueOf(subscriptionID));
//        SdpCommonLogUtil.addPayLoad(", withOffer=", String.valueOf(withOffer));
//        SdpCommonLogUtil.writeDebugLog("Start method loadSubscriptionInfoBySubscriptionID");

        String spName = "pr_personal_annual";
        DAMParam[] param = { new DAMParam("year", year, OracleTypes.VARCHAR),
                            new DAMParam("noOfEmployee", noOfEmployee, OracleTypes.VARCHAR) };
        CSMResultSet rs = null;

        try {

            rs = dam.executeStoredProcedureForQuery(spName, "cur_resultset",
                param);

            /* DebugLog */
            sb = new StringBuilder();
            sb.setLength( 0 );
            sb.append("SP ").append(spName).append(" executed");
            //SdpCommonLogUtil.writeDebugLog(sb);
            System.out.println(sb);

            while (rs.next()) {
//                subscription = Subscription.Factory.newInstance();
//                buildSubscription(rs, subscription, withOffer);
                System.out.print(rs.getString(1));
                System.out.print(rs.getString("NAME"));
                System.out.print(rs.getString(3));
                System.out.print(rs.getString(4));
                System.out.print(rs.getString(5));
                System.out.print(rs.getInt(6));
                System.out.print(rs.getInt(7));
                System.out.print(rs.getInt("ANNUAL_GAP"));
            }
        } catch (SDPException sdp) {
            sdp.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        finally {
            if (rs != null) {
                try {
                    rs.close();
                    rs = null;
                } catch (SQLException e) {
                    rs = null;
                    e.printStackTrace();
//                    ExceptionHandlerManager.handleStoredProcedureException(spName, e);
                }
            }
        }

        /* DebugLog */
//        SdpCommonLogUtil.writeDebugLog("End method loadSubscriptionInfoBySubscriptionID");
        System.out.println("End method loadSubscriptionInfoBySubscriptionID");

    }

}
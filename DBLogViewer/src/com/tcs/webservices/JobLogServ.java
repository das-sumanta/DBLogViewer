package com.tcs.webservices;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.jws.WebMethod;
import javax.jws.WebService;
import javax.persistence.Entity;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;


@WebService()
@Entity
@Path("/joblog")
public class JobLogServ {
	

	@GET
    @Path("retrivejoblog")
    @Produces("text/plain")
    @WebMethod(operationName = "retrivejoblog")
	public String retrivejoblog(@QueryParam("runid") String runID,@QueryParam("etl_date") String etlDate,@QueryParam("entity") String entity) throws SQLException {
		
		ResultSet rs = null;
		PreparedStatement st = null;
		Connection con = null;
        String strHeader = ""; 
        String strData = "";
        String finalStr = "";
        
        
        try 
        {
        
			
			Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://192.168.225.109:3306/wso2devregdb", "devdbusr", "devdbusr");

            //String query = "SELECT job_id, runid, entity, run_mode, ExtractStart, ExtractEnd, S3LoadStart, S3LoadEnd, RedShiftLoadStart, RedShiftLoadEnd, job_status, subsidiary_id FROM job_log;";
            
            String query = "select job_id, runid, entity, run_mode, ExtractStart, ExtractEnd, S3LoadStart, S3LoadEnd, RedShiftLoadStart, RedShiftLoadEnd, job_status, subsidiary_id from job_log a"
            		+ " where a.runid= coalesce(?,a.runid)"
            		+ "and DATE_FORMAT(a.ExtractStart,'%Y-%m-%d') = coalesce(?,DATE_FORMAT(a.ExtractStart,'%Y-%m-%d'))"
            		+ "and a.entity = coalesce(?,a.entity)";
            
            st = con.prepareStatement(query);
            st.setString(1, (runID != null && runID.length() != 0  ? runID:null));
            st.setString(2, (etlDate != null && etlDate.length() != 0  ? etlDate:null));
            st.setString(3,(entity != null && entity.length() != 0 ? entity:null));
            System.out.println("After : " + st.toString());
            rs = st.executeQuery();
            strHeader = "{"
            		+ "\"data\": [";
            int i = 0;
            if(rs.next()) {
            	rs.beforeFirst();            
	            while (rs.next()) 
	            {
	            	if(i == 0) {
	            		strData	= strData	+ "{ "
	            				+ "\"JobId\":" + rs.getInt("job_id") + ","
	            				+ "\"RunId\":" + rs.getInt("runid") + ","
			            		+ "\"Entity\": \"" + rs.getString("entity") + "\","
			            		+ "\"Run Mode\": \"" + rs.getString("run_mode") + "\","
			            		+ "\"ExtractStartTime\": \"" + rs.getTimestamp("ExtractStart") + "\","
			            		+ "\"ExtractEndTime\": \"" + rs.getTimestamp("ExtractEnd") + "\","
			            		+ "\"S3LoadStartTime\": \"" + rs.getTimestamp("S3LoadStart") + "\","
			            		+ "\"S3LoadEndTime\": \"" + rs.getTimestamp("S3LoadEnd") + "\","
			            		+ "\"RedShiftLoadStart\": \"" + rs.getTimestamp("RedShiftLoadStart") + "\","
			            		+ "\"RedShiftLoadEnd\": \"" + rs.getTimestamp("RedShiftLoadEnd") + "\","
			            		+ "\"Job Status\": \"" + rs.getString("job_status") + "\","
			            		+ "\"Subsidiary ID\": \"" + rs.getString("subsidiary_id") + "\""
			            		+ "}";
	            	} else {
	            		strData = strData + "," 
	            				+  "{ "
	            				+ "\"JobId\":" + rs.getInt("job_id") + ","
	            				+ "\"RunId\":" + rs.getInt("runid") + ","
			            		+ "\"Entity\": \"" + rs.getString("entity") + "\","
			            		+ "\"Run Mode\": \"" + rs.getString("run_mode") + "\","
			            		+ "\"ExtractStartTime\": \"" + rs.getTimestamp("ExtractStart") + "\","
			            		+ "\"ExtractEndTime\": \"" + rs.getTimestamp("ExtractEnd") + "\","
			            		+ "\"S3LoadStartTime\": \"" + rs.getTimestamp("S3LoadStart") + "\","
			            		+ "\"S3LoadEndTime\": \"" + rs.getTimestamp("S3LoadEnd") + "\","
			            		+ "\"RedShiftLoadStart\": \"" + rs.getTimestamp("RedShiftLoadStart") + "\","
			            		+ "\"RedShiftLoadEnd\": \"" + rs.getTimestamp("RedShiftLoadEnd") + "\","
			            		+ "\"Job Status\": \"" + rs.getString("job_status") + "\","
			            		+ "\"Subsidiary ID\": \"" + rs.getString("subsidiary_id") + "\""
			            		+ "}";
	            	         		
	            	}
	            	
	            	i++;
	            }
            
	            strData = strData + "] }";
	            finalStr = strHeader + strData;
            } else {
            	finalStr = "No Data Present";
            }
            
        }
		
		catch (Exception e) 
        {
            System.out.println(e.getMessage());
        } finally {
        	
        	if(st != null){
        		st.close();
        	}
        	
        	if(con != null){
        		con.close();
        	}
        }
        
        return finalStr;
		
	}
	@GET
    @Path("retrivemsglog")
    @Produces("text/plain")
    @WebMethod(operationName = "retrivemsglog")
	public String retrivemsglog(@QueryParam("runid") String runID,@QueryParam("etl_date") String etlDate,@QueryParam("entity") String entity) throws SQLException {
		
		ResultSet rs = null;
		PreparedStatement st=null;
		Connection con=null;
        String strHeader = ""; 
        String strData = "";
        String finalStr = "";
		try 
        {
			Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://192.168.225.109:3306/wso2devregdb", "devdbusr", "devdbusr");
            //String query = "SELECT job_id, runid, entity, run_mode, ExtractStart, ExtractEnd, S3LoadStart, S3LoadEnd, RedShiftLoadStart, RedShiftLoadEnd, job_status, subsidiary_id FROM job_log;";
            
            String query = "select message_id, runid, message_desc, target_table, message_stage, message_type, message_timestamp, subsidiary_id from message_log a"
            		+ " where a.runid= coalesce(?,a.runid)"
            		+ "and DATE_FORMAT(a.message_timestamp,'%Y-%m-%d') = coalesce(?,DATE_FORMAT(a.message_timestamp,'%Y-%m-%d'))"
            		+ "and a.target_table = coalesce(?,a.target_table)";
            
            st = con.prepareStatement(query);
            st.setString(1, (runID != null && runID.length() != 0  ? runID:null));
            st.setString(2, (etlDate != null && etlDate.length() != 0 ? etlDate:null));
            st.setString(3,(entity != null && entity.length() != 0 ? entity:null));
            System.out.println("After : " + st.toString());
            rs = st.executeQuery();
            strHeader = "{"
            		+ "\"data\": [";
            int i = 0;
            if(rs.next()) {
            	rs.beforeFirst();            
	            while (rs.next()) 
	            {
	            	if(i == 0) {
	            		strData	= strData	+ "{ "
	            				+ "\"MessageId\":" + rs.getInt("message_id") + ","
	            				+ "\"RunId\":" + rs.getInt("runid") + ","
			            		+ "\"Message Desc\": \"" + rs.getString("message_desc") + "\","
			            		+ "\"Target Table\": \"" + rs.getString("target_table") + "\","
			            		+ "\"Message Stage\": \"" + rs.getString("message_stage") + "\","
			            		+ "\"Message Type\": \"" + rs.getString("message_type") + "\","
			            		+ "\"Message TimeStamp\": \"" + rs.getTimestamp("message_timestamp") + "\","
			            		+ "\"Subsidiary ID\": \"" + rs.getString("subsidiary_id") + "\""
			            		+ "}";
	            	} else {
	            		strData = strData + "," 
	            				+  "{ "
	            				+ "\"MessageId\":" + rs.getInt("message_id") + ","
	            				+ "\"RunId\":" + rs.getInt("runid") + ","
			            		+ "\"Message Desc\": \"" + rs.getString("message_desc") + "\","
			            		+ "\"Target Table\": \"" + rs.getString("target_table") + "\","
			            		+ "\"Message Stage\": \"" + rs.getString("message_stage") + "\","
			            		+ "\"Message Type\": \"" + rs.getString("message_type") + "\","
			            		+ "\"Message TimeStamp\": \"" + rs.getTimestamp("message_timestamp") + "\","
			            		+ "\"Subsidiary ID\": \"" + rs.getString("subsidiary_id") + "\""
			            		+ "}";
	            	         		
	            	}
	            	
	            	i++;
	            }
            
	            strData = strData + "] }";
	            finalStr = strHeader + strData;
            } else {
            	finalStr = "No Data Present";
            }
            
        }
		
		catch (Exception e) 
        {
            System.out.println(e.getMessage());
        }   
		finally {
        	
        	if(st != null){
        		st.close();
        	}
        	
        	if(con != null){
        		con.close();
        	}
        }
        
        return finalStr;
		
	}

	
}

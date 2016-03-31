package com.tcs.webservices;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
	
/*	@GET
    @Path("retrive")
    @Produces("text/html")
    @WebMethod(operationName = "retrive")
    public String retrive() 
    {
        ResultSet rs = null;
        String details = ""; 
        try 
        {
            Class.forName("com.mysql.jdbc.Driver");
            Connection con = DriverManager.getConnection("jdbc:mysql://192.168.225.109:3306/wso2devregdb", "devdbusr", "devdbusr");

            String query = "SELECT job_id, runid, entity, run_mode, ExtractStart, ExtractEnd, S3LoadStart, S3LoadEnd, RedShiftLoadStart, RedShiftLoadEnd, job_status, subsidiary_id FROM job_log;";
            PreparedStatement st = con.prepareStatement(query);
            rs = st.executeQuery();

            details = "<html><body>"; 
            details = details + "<table border=1>";
            details = details + "<tr><td><Strong>JobId </Strong></td>" +
                                    "<td><Strong>RunId </Strong></td>" +
                                     "<td><Strong>Entity </Strong></td>" + 
                                     "<td><Strong>RunMode </Strong></td>" + 
                                     "<td><Strong>ExtractStart </Strong></td>" + 
                                     "<td><Strong>ExtractEnd </Strong></td>" + 
                                     "<td><Strong>S3LoadStart </Strong></td>" + 
                                     "<td><Strong>S3LoadEnd </Strong></td>" + 
                                     "<td><Strong>RedShiftLoadStart </Strong></td>" + 
                                     "<td><Strong>RedShiftLoadEnd </Strong></td>" + 
                                     "<td><Strong>job_status </Strong></td>" + 
                                     "<td><Strong>subsidiary_id </Strong></td>" + "</tr>";
            while (rs.next()) 
            {
                details = details + "<tr><td>" + rs.getInt("job_id") + "</td>" +
                                        "<td>" + rs.getInt("runid") + "</td>"
                                        		+ "<td>" + rs.getString("entity") + "</td>"
                                        				+ "<td>" + rs.getString("run_mode") + "</td>"
                                        						+ "<td>" + rs.getString("ExtractStart") + "</td>"
                                        								+ "<td>" + rs.getString("ExtractEnd") + "</td>"
                                        										+ "<td>" + rs.getString("S3LoadStart") + "</td>"
                                        												+ "<td>" + rs.getString("S3LoadEnd") + "</td>"
                                        														+ "<td>" + rs.getString("RedShiftLoadStart") + "</td>"
                                        																+ "<td>" + rs.getString("RedShiftLoadEnd") + "</td>"
                                        																		+ "<td>" + rs.getString("job_status") + "</td>"
                                        																				+ "<td>" + rs.getString("subsidiary_id") + "</td></tr>";
            }
            details += "</table></body></html>"; 
        } 
        catch (Exception e) 
        {
            System.out.println(e.getMessage());
        }   
        return details;
    }*/

	@GET
    @Path("retrive")
    @Produces("text/plain")
    @WebMethod(operationName = "retrive")
	public String retrive(@QueryParam("runid") int runID,@QueryParam("etl_date") String etlDate,@QueryParam("entity") String entity) {
		
		ResultSet rs = null;
        String strHeader = ""; 
        String strData = "";
        String finalStr = "";
        
        
        
		try 
        {
            
			if(!etlDate.equals("")) {
	        	
	        	DateFormat newEtlDate = new SimpleDateFormat("MM/dd/yyyy"); 
	        	Date dt = newEtlDate.parse(etlDate);
	        	
	        	SimpleDateFormat dt1 = new SimpleDateFormat("yyyy-MM-dd");
	        	etlDate = dt1.format(dt);
	        	
	        }
			
			Class.forName("com.mysql.jdbc.Driver");
            Connection con = DriverManager.getConnection("jdbc:mysql://192.168.225.109:3306/wso2devregdb", "devdbusr", "devdbusr");

            //String query = "SELECT job_id, runid, entity, run_mode, ExtractStart, ExtractEnd, S3LoadStart, S3LoadEnd, RedShiftLoadStart, RedShiftLoadEnd, job_status, subsidiary_id FROM job_log;";
            
            String query = "select job_id, runid, entity, run_mode, ExtractStart, ExtractEnd, S3LoadStart, S3LoadEnd, RedShiftLoadStart, RedShiftLoadEnd, job_status, subsidiary_id from job_log a"
            		+ " where a.runid= coalesce(?,a.runid)"
            		+ "and DATE_FORMAT(a.ExtractStart,'%Y-%m-%d') = coalesce(?,DATE_FORMAT(a.ExtractStart,'%Y-%m-%d'))"
            		+ "and a.entity = coalesce(?,a.entity)";
            
            PreparedStatement st = con.prepareStatement(query);
            st.setInt(1, (runID == 0  ? null:runID));
            st.setString(2, (etlDate.trim() == ""  ? null:etlDate));
            st.setString(3,(entity == "" ? null:entity));
            //System.out.println("After : " + st.toString());
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
        }   
        
        return finalStr;
		
	}

	
}

package com.getprices.truefx;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.sql.Date;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;



public class GetData {
	
	private final static String QUERY_URL_GBP_USD = "http://webrates.truefx.com/rates/connect.html?u=ParkYonggyu&p=fd4936ik&q=rightrates&c=GBP/USD";
	private final static String QUERY_URL_EUR_USD = "http://webrates.truefx.com/rates/connect.html?u=ParkYonggyu&p=fd4936ik&q=rightrates&c=EUR/USD";
	private final static String QUERY_URL_USD_JPY = "http://webrates.truefx.com/rates/connect.html?u=ParkYonggyu&p=fd4936ik&q=rightrates&c=USD/JPY";
	
	private final static String TABLE_GBP_USD = "sti_prices.PricesGBPUSD";
	private final static String TABLE_EUR_USD = "sti_prices.PricesEURUSD";
	private final static String TABLE_USD_JPY = "sti_prices.PricesUSDJPY";
	
	private final static String DATA_URL_FORM = "http://webrates.truefx.com/rates/connect.html?id="; //ParkYonggyu:fd4936ik:rightrates:1455035633258

	private static Logger logger = Logger.getLogger(GetData.class);
	
	private static Connection connStibrokers = null;
	private static PreparedStatement pstmtStiBroker = null;
	
	public static void main(String args[]) {
        
        
        try{ 

            Class.forName(CONSTANTS.driverName); 

            connStibrokers = DriverManager.getConnection(CONSTANTS.dbURLStiBrokers, CONSTANTS.dbURLStiBrokersaUser, CONSTANTS.dbURLStiBrokersPassword); 
            logger.info( DateUtil.getTimeStampString() + " connStibrokers 접속 완료  !!");

            connStibrokers.setAutoCommit(false);
	            
	        
			while ( true ) {
	
				try { 
					queryValueGBPUSD();
					queryValueEURUSD();
					queryValueUSDJPY();
					Thread.sleep(30000);
				} catch(InterruptedException ie) {
				}
			}

		
        } catch ( Exception e ) {
        	
        } finally {
            try {
				

				
				if ( connStibrokers != null ) 
					connStibrokers.close();
				

	            logger.info("conn close");
            } catch (SQLException se) {
            	
            }
        }
		
	}
	
	

	/**
	 * 
	 */
	private static void queryValueGBPUSD() {
		queryValueInsert(QUERY_URL_GBP_USD, TABLE_GBP_USD);
	}
	
	
	
	/**
	 * 
	 */
	private static void queryValueEURUSD() {
		queryValueInsert(QUERY_URL_EUR_USD, TABLE_EUR_USD);
	}
	
	
	
	/**
	 * 
	 */
	private static void queryValueUSDJPY() {
		queryValueInsert(QUERY_URL_USD_JPY, TABLE_USD_JPY);
	}
	
	
	
	/**
	 * 
	 * @param query_url
	 * @param tableName
	 */
	private static void queryValueInsert(String query_url, String tableName) {

		String sell = null;
		String buy = null;
		String high = null;
		String low = null;
		long priceTime = 0L;
		
		String queryId = null;
		String returnValue = null;
		CloseableHttpClient httpclient = HttpClients.createDefault();
		String queryIdURL = null;
		ResponseHandler<String> responseHandler = null; 
		HttpGet httpget = null;
		try {
			httpget = new HttpGet(query_url);
		    // Create a custom response handler
		     responseHandler = new ResponseHandler<String>() {

		        @Override
		        public String handleResponse(
		                final HttpResponse response) throws ClientProtocolException, IOException {
		            int status = response.getStatusLine().getStatusCode();
		            if (status >= 200 && status < 300) {
		                HttpEntity entity = response.getEntity();
		                return entity != null ? EntityUtils.toString(entity) : null;
		            } else {
		                throw new ClientProtocolException("Unexpected response status: " + status);
		            }
		        }

		    };
		    
		    queryId = httpclient.execute(httpget, responseHandler).trim();
		    logger.info( queryId);
		    queryIdURL = URLEncoder.encode(queryId, "UTF-8");
		    
		    logger.info( queryIdURL);
		    httpget = null;
		    responseHandler = null;
			
		    //HttpGet httpget2 = new HttpGet(DATA_URL_FORM + queryIdURL);
		    httpget = new HttpGet(DATA_URL_FORM + queryIdURL);

		    // Create a custom response handler
		    responseHandler = new ResponseHandler<String>() {

		        @Override
		        public String handleResponse(
		                final HttpResponse response) throws ClientProtocolException, IOException {
		            int status = response.getStatusLine().getStatusCode();
		            if (status >= 200 && status < 300) {
		                HttpEntity entity = response.getEntity();
		                return entity != null ? EntityUtils.toString(entity) : null;
		            } else {
		                throw new ClientProtocolException("Unexpected response status: " + status);
		            }
		        }

		    };
		    returnValue = httpclient.execute(httpget, responseHandler);		    
		    
		    
		    logger.info( returnValue);
		  
		    sell = new BigDecimal(returnValue.substring(7, 14)).toString();
		    buy = new BigDecimal(returnValue.substring(14, 21)).toString();
		    high = new BigDecimal(returnValue.substring(21, 28)).toString();
		    low = new BigDecimal(returnValue.substring(28, 35)).toString();
		    priceTime = Long.parseLong(returnValue.substring(35).trim());
		    
		    httpget = null;
		    responseHandler = null;

		    executePreparedStatement( connStibrokers, 
		    		pstmtStiBroker, 
					"insert into " + tableName + " ( streamtime, sell, buy, high, low, yyyy, mm, dd, hh, pricetime, pricetsp ) values ( now(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )",
					sell,
					buy,
					high,
					low,
					priceTime);
		    
		    connStibrokers.commit();
		    
		} catch(SQLException se) {
			se.printStackTrace();
			logger.error("SQLException", se);

		} catch(IOException ie) {
			ie.printStackTrace();
			logger.error("IOException", ie);

		
		} finally {
			try {
				httpclient.close();
				//httpclient2.close();
			} catch(IOException ie) {
			}
		}		
	}	
	
	
	
	/**
	 * 
	 * @param conn
	 * @param pstmt
	 * @param sql
	 * @param sell
	 * @param buy
	 * @param high
	 * @param low
	 * @throws SQLException
	 */
	private static void executePreparedStatement( Connection conn, 
			PreparedStatement pstmt, 
			String sql,
			String sell,
			String buy,
			String high,
			String low,
			long priceTime) throws SQLException {

		int yyyy = 0;
		int mm = 0;
		int dd = 0;
		int hh = 0;
		
	    Calendar cal = Calendar.getInstance();
	    cal.setTimeInMillis(priceTime);
	    yyyy = cal.get(Calendar.YEAR); 
	    mm = cal.get(Calendar.MONTH) + 1;
	    dd = cal.get(Calendar.DAY_OF_MONTH);
	    hh = cal.get(Calendar.HOUR_OF_DAY);
	    Date date = new Date(priceTime);

	    logger.info( "sell : " + sell);
	    logger.info( "buy : " + buy);
	    logger.info( "high : " + high);
	    logger.info( "low : " + low);
	    logger.info( "yyyy : " + yyyy);
	    logger.info( "mm : " + mm);
	    logger.info( "dd : " + dd);
	    logger.info( "hh : " + hh);
	    logger.info( "priceTime : " + priceTime);
	    logger.info( "date : " + date);
	    
	    
	    
		pstmt = conn.prepareStatement(sql);
		pstmt.setString(1, sell);
		pstmt.setString(2, buy);
		pstmt.setString(3, high);
		pstmt.setString(4, low);
		pstmt.setInt(5, yyyy);
		pstmt.setInt(6, mm);
		pstmt.setInt(7, dd);
		pstmt.setInt(8, hh);
		pstmt.setTimestamp(9, new Timestamp(priceTime));
		pstmt.setLong(10, priceTime);
		
		
		pstmt.executeUpdate();
	    if ( pstmt != null ) 
	    	pstmt.close();
	    if ( pstmt != null ) 
	    	pstmt = null;
	}
	

}

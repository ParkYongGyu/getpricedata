package com.getprices.truefx;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {

	
	public static String getTimeStampString() {
	    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
	    return formatter.format(new Date());
	}
	
	public static long getDateTimestamp() {
	    return (new Date()).getTime();
	}	
}

package com.datamelt.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class MessageUtility
{
	private static final String DATETIME_FORMAT	= "yyyy-MM-dd HH:mm:ss";
    private static SimpleDateFormat sdf			= new SimpleDateFormat(DATETIME_FORMAT);
    
    public static String getFormattedMessage(String message)
    {
    	return sdf.format(new Date()) + ": " + message;
    }
}

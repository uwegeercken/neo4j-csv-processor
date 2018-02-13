package com.datamelt.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FileUtility
{
	public static void backupFile(String outputFolder, String fileName) throws IOException 
	{
	    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");

	    File file = new File(outputFolder +"/" + fileName);
	    if(file.exists() && file.isFile())
	    {
	    	File backupFile = new File(outputFolder +"/" + fileName +"_" + sdf.format(new Date().getTime()));
	    	Files.copy(file.toPath(), backupFile.toPath());
	    }
	}
}

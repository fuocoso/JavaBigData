package com.learn.hive;



import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class DateTransform  extends UDF{
	//输入格式： 31/Aug/2015 00:04:37 +0800
	private final SimpleDateFormat inputFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
	//返回格式：  2017-08-07 15:17:45
	private final SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private Text reDate = new Text();
	public Text evaluate(Text inputColumn) {
		if (inputColumn == null) {
			return null;
		}
		//Text "31/Aug/2015:00:04:37 +0800" -> String
		String inDate = inputColumn.toString();
		if(inDate == " ") {
			return null;
		}
		
		// "31/Aug/2015:00:04:37 +0800"  -> 31/Aug/2015:00:04:37
		String strDate = inDate.substring(1, inDate.length()-6);
		
		try {
			Date date =  inputFormat.parse(strDate);
			String formatDate = outputFormat.format(date);
			reDate.set(formatDate);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return reDate;		
	}
	
//	public static void main(String[] args) {
//		String input = "\"31/Aug/2015:00:04:37 +0800\"";
//		Text in = new Text(input);
//		System.out.println(new DateTransform().evaluate(in));
//	}
	
}

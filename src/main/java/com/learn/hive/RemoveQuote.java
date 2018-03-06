package com.learn.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


public class RemoveQuote extends UDF{
	private Text reText = new Text();
	
	public Text evaluate(Text inputColumn) {
		if (inputColumn == null) {
			return null;
		}
		//Text:"27.38.5.159" -> String:"27.38.5.159"
		String inStr = inputColumn.toString();
		if(inStr == " ") {
			return null;
		}
		
		//String:"27.38.5.159" -> String:27.38.5.159
		String reStr = inStr.substring(1,inStr.length()-1);
		//String:27.38.5.159 -> Text:27.38.5.159
		reText.set(reStr);
		return reText;
		
	}
	public static void main(String[] args) {
		System.out.println(new RemoveQuote().evaluate(new Text("\"31/Aug/2015:00:04:54 +0800\"")));

	}

}

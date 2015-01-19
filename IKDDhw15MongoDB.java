
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.MongoCredential;
import com.mongodb.WriteConcern;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.ServerAddress;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;

public class IKDDhw15MongoDB
{
	public static class YAYAYA extends Thread
	{
		String ad="";
		YAYAYA(String a){ad=a;}
		public void run()
		{
			try
			{
				System.out.println("Thread " + ad + " begin");
				MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
				// Now connect to your databases
				DB db = mongoClient.getDB("usergps");
					//System.out.println("Connect to database successfully");
				DBCollection coll = db.getCollection("usergps");					
					
				MongoCredential usergpsAuth = MongoCredential.createPlainCredential("maikaze", "usergps", "12345678".toCharArray());
				List<MongoCredential> auths = new ArrayList<MongoCredential>();
				auths.add(usergpsAuth);
				
				DBObject asda;
				ServerAddress serverAddress = new ServerAddress("localhost", 27017);
					MongoClient mongo = new MongoClient(serverAddress, auths);
				
				File path = new File("/home/maikaze/data/" + ad + "/Trajectory");
		        ArrayList<String> fileList = new ArrayList<String>();
		        if(path.isDirectory())
		        {
		        	String s[]=path.list();
		        	for(int i=0;i<s.length;i++){
		        		fileList.add(s[i]);
		        	}
		        }
		        db.createCollection("usergps",null);
		        
		        String date = "";
		        String time = "";
		        String lat = "";
		        String lon = "";
		        int count = -1;
		        for(int i=0;i<fileList.size();i++)
		        {
		        	FileReader fr = new FileReader("/home/maikaze/data/"+ad+"/Trajectory/" + fileList.get(i));
		        	BufferedReader br = new BufferedReader(fr);
		        	while (br.ready())
		        	{
		        		count++;
		        		String readl = br.readLine();
		        		if(count >= 6)
		        		{
			        		String[] aryS = readl.split(",");
			        		
			        		for(int j = 0; j < aryS.length; j++)
			        		{
			        			if(j == 0)
			        				lat = aryS[j];
			        			if(j == 1)
			        				lon = aryS[j];
			        			if(j == 5)
			        				date = aryS[j];
			        			if(j == 6)
			        				time = aryS[j];
			        		}
			        		BasicDBObject doc = new BasicDBObject("user", ad)
					        .append("date", date)
					        .append("time", time)
					        .append("latitude", lat)
					        .append("longitude", lon);
			        		coll.insert(doc);
		        		}
		        	}
		        	fr.close();
		        	count = -1;
		        }
		        System.out.println("Thread " + ad + " end");
			}
			catch(Exception e)
			{
				System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			}
		}
	}
	public static void main( String args[] )
	{
		try
		{
			MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
			
			DB database = mongoClient.getDB("usergps");
			DBCollection collection = database.getCollection("usergps");
			
			YAYAYA thread[]=new YAYAYA[6];
			for(int i = 0; i < 6; i++)
			{
				thread[i]=new YAYAYA("00"+i);
			}
			long startTimeI = System.currentTimeMillis();
			for(int i = 0; i < 6; i++)
			{
				thread[i].run();
			}
			for(int i = 0; i < 6; i++)
			{
				thread[i].join();
			}
			long endTimeI = System.currentTimeMillis();
			long totTimeI = endTimeI - startTimeI;
			System.out.println("Insert Time:" + totTimeI/1000 + "." + totTimeI%1000 + "s");
						
			BasicDBObject andQuery = new BasicDBObject();
		    List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
		    obj.add(new BasicDBObject("user", "003"));
		    obj.add(new BasicDBObject("date", "2008-11-19"));
		    andQuery.put("$and", obj);
		    
			
			long startTimeF = System.currentTimeMillis();
			DBCursor cursor = collection.find(andQuery);
			long endTimeF = System.currentTimeMillis();
		    long totTimeF = endTimeF - startTimeF;
		    System.out.println("Search Time:" + totTimeF/1000 + "." + totTimeF%1000 + "s");
		    
		    PrintWriter writer = new PrintWriter("output_mongodb.txt", "UTF-8");
		    writer.println("Insert Time:" + totTimeI/1000 + "." + totTimeI%1000 + "s");
		    writer.println("Search Time:" + totTimeF/1000 + "." + totTimeF%1000 + "s");
		    writer.close();
		}
		catch(Exception e)
		{
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
		}
	}
}

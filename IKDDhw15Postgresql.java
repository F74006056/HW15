import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.io.PrintWriter;

public class IKDDhw15Postgresql
{
	public static class InintialDB extends Thread 
	{
		String username = "";
		InintialDB(String s){username = s;}
		public void run()
		{ 
			try
			{
				Class.forName("org.postgresql.Driver");
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
				return;
			}
			Connection connection = null; 
			try
			{
				System.out.println("Thread " + username + " start");
				connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/postgres", "postgres", "123456");
				Statement st = connection.createStatement();
				
				//Get CSV filenames
				File path = new File("/home/maikaze/data/" + username + "/Trajectory");
	        	ArrayList<String> fileList = new ArrayList<String>();
	        	if(path.isDirectory())
	        	{
	        		String s[]=path.list();
		        	for(int i=0;i<s.length;i++)
		        		fileList.add(s[i]);
			    }
	        	//Read files
	        	String date = "";
		        String timing = "";
		        String lat = "";
		        String lon = "";
		        Date time = new Date();
	        	int count = -1;
	        	SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
			    for(int i=0;i<fileList.size();i++)
			    {
			    	FileReader fr = new FileReader("/home/maikaze/data/" + username + "/Trajectory/" + fileList.get(i));
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
			        			{
			        				timing = aryS[j];
			        				time = sdf.parse(timing);
			        			}
			        		}
			        		String insertSQL = "INSERT INTO usergps VALUES(" + "\'" + username + "\'" + ", " + "\'" + date + "\'" + ", " + "\'" + timing + "\'" + ", " + lat + ", " + lon + ")";
			        		st.executeUpdate(insertSQL);
		        		}
		        	}
		        	fr.close();
		        	count = -1;
			    }
			    connection.close();
			    System.out.println("Thread " + username + " finish");
			}
			
			catch(Exception e)
			{
				System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			}
	 
			if (connection == null)
				System.out.println("Failed to make connection!");
		}
	}
	public static void main(String args[])
	{
		try
		{
			Class.forName("org.postgresql.Driver");
		}
		catch (ClassNotFoundException e)
		{
			e.printStackTrace();
			return;
		}
		Connection connection = null;
		try
		{
			connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/postgres", "postgres", "123456");
			Statement st = connection.createStatement();
			

			String dropgpsSQL = "DROP TABLE IF EXISTS usergps";
			st.executeUpdate(dropgpsSQL);
			
			//Create table
			String createuserSQL = "CREATE TABLE usergps (UserName varchar(20), Date varchar(20), Time time, Latitude decimal(11, 7), Longtitude decimal(11, 7))";
			st.executeUpdate(createuserSQL);
			
			InintialDB thread[]=new InintialDB[6];
			for(int i = 0; i < 6; i++)
			{
				thread[i]=new InintialDB("00"+i);
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
			String selectSQL = "SELECT * FROM usergps WHERE username='003' AND date='2008-11-19'";
			long startTimeF = System.currentTimeMillis();
			ResultSet selectdbResult = st.executeQuery(selectSQL);
			long endTimeF = System.currentTimeMillis();
			long totTimeF = endTimeF - startTimeF;
			System.out.println("Search Time:" + totTimeF/1000 + "." + totTimeF%1000 + "s");
			connection.close();
			
			PrintWriter writer = new PrintWriter("output_postgresql.txt", "UTF-8");
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

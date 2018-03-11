/**
 * 
 */
package org.bgu.ise.ddb.items;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.MediaItems;
import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;



/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/items")
public class ItemsController extends ParentController {
	
	
	
	/**
	 * The function copy all the items(title and production year) from the Oracle table MediaItems to the System storage.
	 * The Oracle table and data should be used from the previous assignment
	 */
	@RequestMapping(value = "fill_media_items", method={RequestMethod.GET})
	public void fillMediaItems(HttpServletResponse response){
		System.out.println("was here");
		//:TODO your implementation
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			String connectionURL = "jdbc:oracle:thin:@ora1.ise.bgu.ac.il:1521/ORACLE";
			Connection conn = DriverManager.getConnection(connectionURL, "talshko","abcd");
			System.out.println("*****3************");
			String query = "SELECT * FROM MediaItems";
			PreparedStatement pStatement = conn.prepareStatement(query);
			ResultSet rs = pStatement.executeQuery();
			
			while(rs.next()){
				MongoClient mongoClient = new MongoClient( "localhost" , 27017);
				DB  database = mongoClient.getDB("TalMoranDB");
				DBCollection  mediaItems = database.getCollection("MEDIAITEMS");
				String title = rs.getString("title");
				int production_year = rs.getInt("PROD_YEAR");
				BasicDBObject  item = new BasicDBObject();
				item.put("title", title);
				item.put("production_year", production_year);
				mediaItems.insert(item);
				mongoClient.close();
			}
			rs.close();
			pStatement.close();
			conn.close();
		} catch (Exception e) {
			// TODO: handle exception
		}
		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}
	
	

	/**
	 * The function copy all the items from the remote file,
	 * the remote file have the same structure as the films file from the previous assignment.
	 * You can assume that the address protocol is http
	 * @throws IOException 
	 */
	@RequestMapping(value = "fill_media_items_from_url", method={RequestMethod.GET})
	public void fillMediaItemsFromUrl(@RequestParam("url")    String urladdress,
			HttpServletResponse response) throws IOException{
		System.out.println(urladdress);
		
		//:TODO your implementation
		try {
			URL oracle = new URL(urladdress);
	        BufferedReader in = new BufferedReader(
	        new InputStreamReader(oracle.openStream()));
	        String inputLine;
	        while ((inputLine = in.readLine()) != null) {
	        	MongoClient mongoClient = new MongoClient( "localhost" , 27017);
				DB  database = mongoClient.getDB("TalMoranDB");
				DBCollection  mediaItems = database.getCollection("MEDIAITEMS");
	        	System.out.println(inputLine);
	        	String [] line = inputLine.split(",");
	        	String title = line[0];
	        	System.out.println(title);
	        	int year =Integer.parseInt(line[1]);
	        	System.out.println(year);
				BasicDBObject  item = new BasicDBObject();
				item.put("title", title);
				item.put("production_year",year);
				mediaItems.insert(item);
				mongoClient.close();
	        }  
	        in.close();
		} catch (Exception e) {
			// TODO: handle exception
		}
		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}
	
	
	/**
	 * The function retrieves from the system storage N items,
	 * order is not important( any N items) 
	 * @param topN - how many items to retrieve
	 * @return
	 */
	@RequestMapping(value = "get_topn_items",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(MediaItems.class)
	public  MediaItems[] getTopNItems(@RequestParam("topn")    int topN){
		//:TODO your implementation
		List<MediaItems> list = new ArrayList<MediaItems>();
		try {
			MongoClient mongoClient = new MongoClient( "localhost" , 27017);
			DB  database = mongoClient.getDB("TalMoranDB");
			DBCollection  mediaItems = database.getCollection("MEDIAITEMS");
			DBCursor answer = mediaItems.find();
			int counter = 0;
			while(answer.hasNext() && counter<topN) {
				DBObject item =	answer.next();
				String title = (String)item.get("title");
				int production_year = (int)item.get("production_year");
				
				MediaItems itemObj = new MediaItems(title,production_year);
				list.add(itemObj);
				counter = counter +1;
			}
	        mongoClient.close();
		}
		catch (MongoException e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		MediaItems [] array = new MediaItems[list.size()];
		array = list.toArray(array);
		return array;
	}
		

}

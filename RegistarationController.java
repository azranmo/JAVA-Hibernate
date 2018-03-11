/**
 * 
 */
package org.bgu.ise.ddb.registration;


import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Calendar;
import java.util.Date;


import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
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
import com.mongodb.Function;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.sun.javafx.collections.MappingChange.Map;
/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/registration")
public class RegistarationController extends ParentController{
	
	
	/**
	 * The function checks if the username exist,
	 * in case of positive answer HttpStatus in HttpServletResponse should be set to HttpStatus.CONFLICT,
	 * else insert the user to the system  and set to HttpStatus in HttpServletResponse HttpStatus.OK
	 * @param username
	 * @param password
	 * @param firstName
	 * @param lastName
	 * @param response
	 * @throws UnknownHostException 
	 */
	@RequestMapping(value = "register_new_customer", method={RequestMethod.POST})
	public void registerNewUser(@RequestParam("username") String username,
			@RequestParam("password")    String password,
			@RequestParam("firstName")   String firstName,
			@RequestParam("lastName")  String lastName,
			HttpServletResponse response){
		System.out.println(username+" "+password+" "+lastName+" "+firstName);
		//:TODO your implementation
		try {
			if(isExistUser(username)) {
				HttpStatus status = HttpStatus.CONFLICT;
				response.setStatus(status.value());
			}
			else {  
				MongoClient mongoClient;
					mongoClient = new MongoClient( "localhost" , 27017);
					DB  database = mongoClient.getDB("TalMoranDB");
					DBCollection  users = database.getCollection("USERS");
					BasicDBObject user = new BasicDBObject();
					user.put("username", username);
					user.put("password", password);
					user.put("firstName", firstName);
					user.put("lastName", lastName);
					Date date = new Date();
					user.put("date",date );
					users.insert(user);
					mongoClient.close();
					HttpStatus status = HttpStatus.OK;
					response.setStatus(status.value());
			}
		}catch (MongoException e) {
			e.printStackTrace();
			// TODO: handle exception
			//System.out.println(e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * The function returns true if the received username exist in the system otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "is_exist_user", method={RequestMethod.GET})
	public boolean isExistUser(@RequestParam("username") String username) throws IOException{
		System.out.println(username);
		boolean result = false;
		//:TODO your implementation
		try {
			MongoClient mongoClient = new MongoClient( "localhost" , 27017);
			DB  database = mongoClient.getDB("TalMoranDB");
			DBCollection  users = database.getCollection("USERS");
			BasicDBObject setquery = new BasicDBObject();
			setquery.put("username", username);

			DBCursor answer = users.find(setquery);

			if (answer.hasNext()) {
			    result = true;
			} 
	        mongoClient.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		return result;
		
	}
	
	/**
	 * The function returns true if the received username and password match a system storage entry, otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "validate_user", method={RequestMethod.POST})
	public boolean validateUser(@RequestParam("username") String username,
			@RequestParam("password")    String password) throws IOException{
		System.out.println(username+" "+password);
		boolean result = false;
		//:TODO your implementation
		try {
			MongoClient mongoClient = new MongoClient( "localhost" , 27017);
			DB  database = mongoClient.getDB("TalMoranDB");
			DBCollection  users = database.getCollection("USERS");
			BasicDBObject setquery = new BasicDBObject();
			setquery.put("username", username);
			setquery.put("password", password);
			DBCursor answer = users.find(setquery);
			if (answer.hasNext()) {
			    result = true;
			} 
	        mongoClient.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		
		return result;
		
	}
	
	/**
	 * The function retrieves number of the registered users in the past n days
	 * @param days
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "get_number_of_registred_users", method={RequestMethod.GET})
	public int getNumberOfRegistredUsers(@RequestParam("days") int days) throws IOException{
		System.out.println(days+"");
		int result = 0;
		//:TODO your implementation
		try {
			
			MongoClient mongoClient = new MongoClient( "localhost" , 27017);
			DB  database = mongoClient.getDB("TalMoranDB");
			DBCollection  users = database.getCollection("USERS");
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DATE, -days);
			Date dif = cal.getTime();
			System.out.println(dif);
			BasicDBObject difquery = new BasicDBObject();
			BasicDBObject setquery = new BasicDBObject("$gt", dif);
			difquery.put("date", setquery);
			DBCursor answer = users.find(difquery);
			System.out.println("************");
			result = answer.count();
			System.out.println(result);
	        mongoClient.close();
		}
		catch (MongoException e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return result;
		
	}
	
	/**
	 * The function retrieves all the users
	 * @return
	 */
	@RequestMapping(value = "get_all_users",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(User.class)
	public  User[] getAllUsers(){
		//:TODO your implementation
		List<User> list = new ArrayList<User>();
		try {
			MongoClient mongoClient = new MongoClient( "localhost" , 27017);
			DB  database = mongoClient.getDB("TalMoranDB");
			DBCollection  users = database.getCollection("USERS");
			DBCursor answer = users.find();
			while(answer.hasNext()) {
				DBObject user =	answer.next();
				String username = (String)user.get("username");
				String firstName = (String)user.get("firstName");
				String lastName = (String)user.get("lastName");
				User userObj = new User(username,firstName,lastName);
				list.add(userObj);
			}
	        mongoClient.close();
		}
		catch (MongoException e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		User [] array = new User[list.size()];
		array = list.toArray(array);
		return array;
	}

}
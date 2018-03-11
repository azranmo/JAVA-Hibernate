/**
 * 
 */
package org.bgu.ise.ddb.history;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.bgu.ise.ddb.registration.RegistarationController;
import org.bson.Document;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.sun.scenario.effect.impl.sw.sse.SSEBlend_COLOR_BURNPeer;

/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/history")
public class HistoryController extends ParentController{
	
	
	
	/**
	 * The function inserts to the system storage triple(s)(username, title, timestamp). 
	 * The timestamp - in ms since 1970
	 * Advice: better to insert the history into two structures( tables) in order to extract it fast one with the key - username, another with the key - title
	 * @param username
	 * @param title
	 * @param response
	 */
	@RequestMapping(value = "insert_to_history", method={RequestMethod.GET})
	public void insertToHistory (@RequestParam("username")    String username,
			@RequestParam("title")   String title,
			HttpServletResponse response){
		System.out.println(username+" "+title);
		//:TODO your implementation
		try {
			if(isExistUser(username) && isExistMovie(title)) {
				if(isExistInHistoryByUser(username)==false) {
					MongoClient mongoClient = new MongoClient( "localhost" , 27017);
					DB  database = mongoClient.getDB("TalMoranDB");
					DBCollection  historyUserCollection = database.getCollection("HISTORY_BY_USER");
					BasicDBObject historyItem = new BasicDBObject();
					historyItem.put("username", username);
					List<BasicDBObject> movies = new ArrayList();
					BasicDBObject movie = new BasicDBObject();
					movie.put("title", title);
					Date now = new Date();
					movie.put("timestamp", now);
					movies.add(movie);
					historyItem.put("movies",movies);
					historyUserCollection.insert(historyItem);
					mongoClient.close();
				}
				else {
					MongoClient mongoClient = new MongoClient( "localhost" , 27017);
					DB  database = mongoClient.getDB("TalMoranDB");
					DBCollection  historyUserCollection = database.getCollection("HISTORY_BY_USER");
					BasicDBObject movie = new BasicDBObject();
					movie.put("title", title);
					Date now = new Date();
					movie.put("timestamp", now);
					
					BasicDBList historyItemList = new BasicDBList();
					historyItemList.add(movie);
					BasicDBObject historyItemObj = new BasicDBObject();
					historyItemObj = historyItemObj.append("username", username);
					BasicDBObject movies = new BasicDBObject();
					movies.put("$each", historyItemList);
					movies.put("$sort", new BasicDBObject("timestamp", -1));
					BasicDBObject toPush = new BasicDBObject("$push", new BasicDBObject("movies", movies));
					historyUserCollection.update(historyItemObj, toPush);
					
					mongoClient.close();
				}
				if(isExistInHistoryByMovie(title) == false) {
					MongoClient mongoClient = new MongoClient( "localhost" , 27017);
					DB  database = mongoClient.getDB("TalMoranDB");
					DBCollection  historyMovieCollection = database.getCollection("HISTORY_BY_MOVIE");
					BasicDBObject historyItem = new BasicDBObject();
					historyItem.put("title", title);
					List<BasicDBObject> users = new ArrayList();
					BasicDBObject user = new BasicDBObject();
					user.put("username", username);
					Date now = new Date();
					user.put("timestamp", now);
					users.add(user);
					historyItem.put("users",users);
					historyMovieCollection.insert(historyItem);
					mongoClient.close();
				}
				else {
					MongoClient mongoClient = new MongoClient( "localhost" , 27017);
					DB  database = mongoClient.getDB("TalMoranDB");
					DBCollection  historyMovieCollection = database.getCollection("HISTORY_BY_MOVIE");
					BasicDBObject user = new BasicDBObject();
					user.put("username", username);
					Date now = new Date();
					user.put("timestamp", now);
					
					BasicDBList historyItemList = new BasicDBList();
					historyItemList.add(user);
					BasicDBObject historyItemObj = new BasicDBObject();
					historyItemObj = historyItemObj.append("title", title);
					BasicDBObject users = new BasicDBObject();
					users.put("$each", historyItemList);
					users.put("$sort", new BasicDBObject("timestamp", -1));
					BasicDBObject toPush = new BasicDBObject("$push", new BasicDBObject("users", users));
					historyMovieCollection.update(historyItemObj, toPush);
					mongoClient.close();
				}
			}
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}
	
	/**
	 * The function checks if username exists in db HistoryByUser
	 * @param username
	 * @return true if exists, false if doesn't.
	 * @throws IOException
	 */
	private boolean isExistInHistoryByUser(String username) throws IOException{
		boolean result = false;
		try {
			MongoClient mongoClient = new MongoClient( "localhost" , 27017);
			DB  database = mongoClient.getDB("TalMoranDB");
			DBCollection  users = database.getCollection("HISTORY_BY_USER");
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
		System.out.println(result);
		return result;
		
	}
	
	/**
	 * The function checks if username exists in db HistoryByTitle
	 * @param title
	 * @return true if exists, false if doesn't.
	 * @throws IOException
	 */
	private boolean isExistInHistoryByMovie(String title) throws IOException{
		boolean result = false;
		try {
			MongoClient mongoClient = new MongoClient( "localhost" , 27017);
			DB  database = mongoClient.getDB("TalMoranDB");
			DBCollection  users = database.getCollection("HISTORY_BY_MOVIE");
			BasicDBObject setquery = new BasicDBObject();
			setquery.put("title", title);
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
		System.out.println(result);
		return result;
		
	}
	
	/**
	 * The function checks if username exists in db USERS
	 * @param username
	 * @return  true if exists, false if doesn't.
	 * @throws IOException
	 */
	private boolean isExistUser(String username) throws IOException{
		boolean result = false;
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
	 * The function checks if username exists in db MEDIAITEMS
	 * @param title
	 * @return true if exists, false if doesn't.
	 * @throws IOException
	 */
	private boolean isExistMovie(String title) throws IOException{
		boolean result = false;
		try {
			MongoClient mongoClient = new MongoClient( "localhost" , 27017);
			DB  database = mongoClient.getDB("TalMoranDB");
			DBCollection  mediaitems = database.getCollection("MEDIAITEMS");
			BasicDBObject setquery = new BasicDBObject();
			setquery.put("title", title);
			DBCursor answer = mediaitems.find(setquery);
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
	 * The function retrieves  users' history
	 * The function return array of pairs <title,viewtime> sorted by VIEWTIME in descending order
	 * @param username
	 * @return
	 */
	@RequestMapping(value = "get_history_by_users",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  HistoryPair[] getHistoryByUser(@RequestParam("entity")    String username){
		//:TODO your implementation
		List<HistoryPair> list = new ArrayList<HistoryPair>();
		try {
			if(isExistInHistoryByUser(username)) {
				MongoClient client = new MongoClient("localhost", 27017);
				MongoDatabase database = client.getDatabase("TalMoranDB");
				MongoCollection<Document> collection = database.getCollection("HISTORY_BY_USER");

				List<Document> historyUsers = (List<Document>) collection.find().into(new ArrayList<Document>());
				for (Document history : historyUsers) {
					String usernameDoc = (String)history.get("username");
					if(usernameDoc.equals(username)) {
						List<Document> movies = (List<Document>) history.get("movies");
						for (Document movieDoc:movies ) {
							String titleMovie = (String) movieDoc.get("title");
							Date timestampMovie = (Date)movieDoc.get("timestamp");
							HistoryPair hp = new HistoryPair(titleMovie, timestampMovie);
							list.add(hp);
						}
						break;
					}
				}
				client.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		HistoryPair [] array = new HistoryPair[list.size()];
		array = list.toArray(array);
		return array;
	}
	
	
	/**
	 * The function retrieves  items' history
	 * The function return array of pairs <username,viewtime> sorted by VIEWTIME in descending order
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_history_by_items",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  HistoryPair[] getHistoryByItems(@RequestParam("entity")    String title){
		//:TODO your implementation
		List<HistoryPair> list = new ArrayList<HistoryPair>();
		try {
			if(isExistInHistoryByMovie(title)) {
				MongoClient client = new MongoClient("localhost", 27017);
				MongoDatabase database = client.getDatabase("TalMoranDB");
				MongoCollection<Document> collection = database.getCollection("HISTORY_BY_MOVIE");

				List<Document> historyMovies = (List<Document>) collection.find().into(new ArrayList<Document>());
				for (Document history : historyMovies) {
					String titleDoc = (String)history.get("title");
					if(titleDoc.equals(title)) {
						List<Document> users = (List<Document>) history.get("users");
						for (Document userDoc:users ) {
							String usernameMovie = (String) userDoc.get("username");
							Date timestampMovie = (Date)userDoc.get("timestamp");
							HistoryPair hp = new HistoryPair(usernameMovie, timestampMovie);
							list.add(hp);
						}
						break;
					}
				}
				client.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		HistoryPair [] array = new HistoryPair[list.size()];
		array = list.toArray(array);
		return array;
	}
	
	/**
	 * The function retrieves all the  users that have viewed the given item
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_users_by_item",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  User[] getUsersByItem(@RequestParam("title") String title){
		//:TODO your implementation
		List<User> list = new ArrayList<User>();
		try {
			if(isExistInHistoryByMovie(title)) {
				MongoClient client = new MongoClient("localhost", 27017);
				MongoDatabase database = client.getDatabase("TalMoranDB");
				MongoCollection<Document> collection = database.getCollection("HISTORY_BY_MOVIE");

				List<Document> historyMovies = (List<Document>) collection.find().into(new ArrayList<Document>());
				for (Document history : historyMovies) {
					String titleDoc = (String)history.get("title");
					if(titleDoc.equals(title)) {
						List<Document> users = (List<Document>) history.get("users");
						for (Document userDoc:users ) {
							String usernameMovie = (String) userDoc.get("username");
							
							MongoClient mongoClient = new MongoClient( "localhost" , 27017);
							DB  db = mongoClient.getDB("TalMoranDB");
							DBCollection  usersCollection = db.getCollection("USERS");
							BasicDBObject setquery = new BasicDBObject();
							setquery.put("username", usernameMovie);

							DBCursor answer = usersCollection.find(setquery);

							if (answer.hasNext()) {
								DBObject user =	answer.next();
								String username = (String)user.get("username");
								String firstName = (String)user.get("firstName");
								String lastName = (String)user.get("lastName");
								User userObj = new User(username,firstName,lastName);
								list.add(userObj);
							} 
					        mongoClient.close();
						}
						break;
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		User [] array = new User[list.size()];
		array = list.toArray(array);
		return array;
	}
	
	/**
	 * The function calculates the similarity score using Jaccard similarity function:
	 *  sim(i,j) = |U(i) intersection U(j)|/|U(i) union U(j)|,
	 *  where U(i) is the set of usernames which exist in the history of the item i.
	 * @param title1
	 * @param title2
	 * @return
	 */
	@RequestMapping(value = "get_items_similarity",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	public double  getItemsSimilarity(@RequestParam("title1") String title1,
			@RequestParam("title2") String title2){
		double ret = 0.0;
		try {
			if(isExistInHistoryByMovie(title1) && isExistInHistoryByMovie(title2)) {
				User [] usersLikesTitle1 = getUsersByItem(title1);
				User [] usersLikesTitle2 = getUsersByItem(title2);
				double counterUnion = 0;
				double counterIntersection = 0;
				for (User u1 : usersLikesTitle1) {
					for (User u2 : usersLikesTitle2) {
						if(u1.getUsername().equals(u2.getUsername())) {
							counterIntersection++;
						}
					}
				}
				counterUnion =usersLikesTitle1.length + usersLikesTitle2.length - counterIntersection;
				ret = counterIntersection/counterUnion;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//:TODO your implementation
		
		return ret;
	}
	

}

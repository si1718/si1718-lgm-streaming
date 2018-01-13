package db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import data.streaming.dto.RatingDTO;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.bson.Document;
import org.bson.conversions.Bson;
public class MongoConnection {

    private static final MongoClientURI uri = new MongoClientURI(Utils.URL_DATABASE);
    private static MongoClient client;
    private static MongoDatabase database;
    private static MongoCollection<Document> tweetsCollection;
    private static MongoCollection<Document> booksCollection;
	private static MongoCollection<Document> ratingsCollection;
	private static MongoCollection<Document> recommendationsCollection;
	private static MongoCollection<Document> tweetsDataDateCollection;
	private static MongoCollection<Document> tweetsDataMonthCollection;

    public static SortedSet<String> getKeywords() {
    	openDB();
    	MongoCollection<org.bson.Document> collection = database.getCollection("books");
    	SortedSet<String> keywordsRaw = collection.distinct("keywords", String.class).into(new TreeSet<>());
    	SortedSet<String> keywords = new TreeSet<>();
    	
    	for(String i : keywordsRaw) {
    		String[] raw = i.split(",");
    		
    		for(String e : raw) {
    			if(!e.isEmpty()) {
    			
    			keywords.add(e.trim());
    			}
    		}
    	}
    	return keywords;
    }
    	
    public static void openDB() {
    	if(client != null) {
    		return;
    	}
    	client = new MongoClient(uri);
		database = client.getDatabase("si1718-lgm-books");
    }
    
    public static void closeDB() {
    	client.close();
    }
    
    public static void openTweetsConnection() {
    	if(tweetsCollection!=null) {
    		return;
    	}
		openDB();
		MongoCollection<Document> collection = database.getCollection("BookTweets");
		MongoConnection.tweetsCollection = collection;
	}
    
    public static boolean saveRatings(List<RatingDTO> ratings) {
		System.out.println(ratings.size());
    	ratings.forEach(x -> {
			try {
				saveRating(x);
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
		return true;
	}
    
    public static boolean saveRating(RatingDTO rating) throws JsonProcessingException {
		openRatingsConnection();
		boolean existing = false;
		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(rating);
		Document doc = Document.parse(json);
		
		Iterable<Document> documents = MongoConnection.getAllRatings();
		List<Document> ratings = new ArrayList<>();
		documents.forEach(
				x -> ratings.add(x));
		for(Integer i=0;i<ratings.size();i++) {
			String ratingId = (String) ratings.get(i).get("id");
			if(ratingId.equals(rating.getId())) {
				existing = true;
				break;
			}
		}
		if(existing==false) {
			ratingsCollection.insertOne(doc);
			System.out.println("Insertado");
		}
		return true;
	}
    
    public static boolean saveRatingRecommendation(RatingDTO rating) throws JsonProcessingException {
		openRatingsRecommendationConnection();
		boolean existing = false;
		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(rating);
		Document doc = Document.parse(json);
		
		Iterable<Document> documents = MongoConnection.getAllRatingsRecommendations();
		List<Document> ratings = new ArrayList<>();
		documents.forEach(
				x -> ratings.add(x));
		for(Integer i=0;i<ratings.size();i++) {
			String ratingId = (String) ratings.get(i).get("id");
			if(ratingId.equals(rating.getId())) {
				existing = true;
				break;
			}
		}
		if(existing==false) {
			recommendationsCollection.insertOne(doc);
			System.out.println("Insertado");
		}
		return true;
	}
    
	public static void openTweetsDataMonthConnection() {
		openDB();
		MongoCollection<Document> collection = database.getCollection("tweetsDataMonth");
		MongoConnection.tweetsDataMonthCollection = collection;
	}
	
	public static void openTweetsDataDateConnection() {
		openDB();
		MongoCollection<Document> collection = database.getCollection("tweetsDataDate");
		MongoConnection.tweetsDataDateCollection = collection;
	}
	
	public static boolean saveTweetsData(Map<String,Map<String,Integer>> mapResult,Integer op) throws JsonProcessingException{
		if(op==0) {
			Iterable<Document> documents = MongoConnection.getAllDates();
			List<Document> dates = new ArrayList<>();
			documents.forEach(
					x -> dates.add(x));
			Set<String> dateKeys = mapResult.keySet();
			for(Document d:dates) {
				for(String s:dateKeys) {
					if(d.get(s)!=null) {
						Integer bus = 0;
						Document i = (Document) d.get(s);
						Set<String> keywordsDate = mapResult.get(s).keySet();
						for(String kD:keywordsDate) {
							if(kD.equals("business")) {
								bus = (Integer) i.get(kD);
							}
							Integer suma = (Integer) i.get(kD) + mapResult.get(s).get(kD);
							mapResult.get(s).put(kD, suma);
						}
						
						String auxBus = s+".business";
						Bson filter = Filters.eq(auxBus, bus);
						tweetsDataDateCollection.deleteOne(filter);	
					}
				}
			}
			
			openTweetsDataDateConnection();
			ObjectMapper mapper = new ObjectMapper();
			String json = mapper.writeValueAsString(mapResult);
			Document doc = Document.parse(json);
			tweetsDataDateCollection.insertOne(doc);
			
		}else if(op==1){
			Iterable<Document> documents = MongoConnection.getAllMonths();
			List<Document> months = new ArrayList<>();
			documents.forEach(
					x -> months.add(x));
			Set<String> monthKeys = mapResult.keySet();
			for(Document d:months) {
				for(String s:monthKeys) {
					if(d.get(s)!=null) {
						Integer bus = 0;
						Document i = (Document) d.get(s);
						Set<String> keywordsMonth = mapResult.get(s).keySet();
						for(String kD:keywordsMonth) {
							if(kD.equals("business")) {
								bus = (Integer) i.get(kD);
							}
							Integer suma = (Integer) i.get(kD) + mapResult.get(s).get(kD);
							mapResult.get(s).put(kD, suma);
						}
						
						String auxBus = s+".business";
						Bson filter = Filters.eq(auxBus, bus);
						tweetsDataMonthCollection.deleteOne(filter);	
					}
				}
			}
			
			openTweetsDataMonthConnection();
			ObjectMapper mapper = new ObjectMapper();
			String json = mapper.writeValueAsString(mapResult);
			Document doc = Document.parse(json);
			tweetsDataMonthCollection.insertOne(doc);
		}
		return true;
	}
	
	public static void openRatingsConnection() {
		openDB();
		MongoCollection<Document> collection = database.getCollection("ratings");
		MongoConnection.ratingsCollection = collection;
	}
	
	public static void openRatingsRecommendationConnection() {
		openDB();
		MongoCollection<Document> collection = database.getCollection("recommendations");
		MongoConnection.recommendationsCollection = collection;
	}
	
	public static Iterable<Document> getAllRatings(){
		openRatingsConnection();
		FindIterable<Document> result = ratingsCollection.find();
		return result;
	}
	
	public static Iterable<Document> getAllRatingsRecommendations(){
		openRatingsConnection();
		FindIterable<Document> result = recommendationsCollection.find();
		return result;
	}
	
	public static Set<RatingDTO> getAllRatingsRecommendation(){
		openRatingsConnection();
		FindIterable<Document> result = ratingsCollection.find();
		Set<RatingDTO> ratings = new HashSet<>();
		for(Document d:result) {
			String isbn1 = (String) d.get("isbn1");
			String isbn2 = (String) d.get("isbn2");
			Integer rating = (Integer) d.get("rating");
			String id = (String) d.get("id");
			RatingDTO rdto = new RatingDTO(isbn1,isbn2,rating,id);
			ratings.add(rdto);
		}
		return ratings;
	}
	
    public static MongoCollection<Document> getBooksCollection() {
		openBooksConnection();
		return booksCollection;
	}
    
    public static Iterable<Document> getAllBooks(){
		openBooksConnection();
		FindIterable<Document> result = booksCollection.find();
		return result;
	}
    
    public static Iterable<Document> getAllMonths(){
    	openTweetsDataMonthConnection();
		FindIterable<Document> result = tweetsDataMonthCollection.find();
		return result;
	}
    
    public static Iterable<Document> getAllDates(){
    	openTweetsDataDateConnection();
		FindIterable<Document> result = tweetsDataDateCollection.find();
		return result;
	}
    
    public static void openBooksConnection() {
		openDB();
		MongoCollection<Document> collection = database.getCollection("books");
		MongoConnection.booksCollection = collection;
	}
    
    public static Iterable<Document> getAlltweets(){
		openTweetsConnection();
		//FindIterable<Document> result = tweetsCollection.find().skip(0).limit(100);
		FindIterable<Document> result = tweetsCollection.find();
		return result;
	}
    
    
    
    public void populateCollection(String collection, Document data) {
    	openDB();
    	database.getCollection(collection).insertOne(data);
    	//this.closeDB();
    }
}

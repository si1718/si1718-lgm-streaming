package data.streaming.utils;

import java.io.IOException;

import org.bson.Document;

import com.fasterxml.jackson.databind.ObjectMapper;

import data.streaming.dto.TweetDTO;
import db.MongoConnection;

public class Utils {
	
	
	//public static final String[] TAG_NAMES = { "#OTDirecto12D", "#InmaculadaConcepcion" };
	private static final ObjectMapper mapper = new ObjectMapper();
	private static MongoConnection mongoConnection;

	public static TweetDTO createTweetDTO(String x) {
		TweetDTO result = null;

		try {
			result = mapper.readValue(x, TweetDTO.class);
		} catch (IOException e) {

		}
		return result;
	}
	
	public static boolean saveTweetInDB(String x) {
		if(mongoConnection == null) {
			mongoConnection = new MongoConnection();
		}
		Document tweet = Document.parse(x);
		mongoConnection.populateCollection("BookTweets", tweet);
		return true;
	}
	
	
	public static boolean isTweetValid(String x) {
		boolean result = true;
		
		try {
			mapper.readValue(x,TweetDTO.class);
		}catch(IOException ignored) {
			result = false;
		}
		
		return result;
	}

}

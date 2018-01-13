package batch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

import org.apache.flink.shaded.com.google.common.collect.Maps;
import org.bson.Document;
import org.grouplens.lenskit.ItemRecommender;
import org.grouplens.lenskit.ItemScorer;
import org.grouplens.lenskit.Recommender;
import org.grouplens.lenskit.RecommenderBuildException;
import org.grouplens.lenskit.core.LenskitConfiguration;
import org.grouplens.lenskit.core.LenskitRecommender;
import org.grouplens.lenskit.data.dao.EventCollectionDAO;
import org.grouplens.lenskit.data.dao.EventDAO;
import org.grouplens.lenskit.data.event.Event;
import org.grouplens.lenskit.data.event.MutableRating;
import org.grouplens.lenskit.knn.user.UserUserItemScorer;
import org.grouplens.lenskit.scored.ScoredId;

import com.fasterxml.jackson.core.JsonProcessingException;

import data.streaming.dto.RatingDTO;
import db.MongoConnection;

public class Batchman implements Runnable{
	private static String[] months=new String[]{"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"};
	private static String[] values=new String[]{"1","2","3","4","5","6","7","8","9","10","11","12"};
	private static Map<String,String> mapMonth;
	private static Map<String,Map<String,Integer>> mapDateResult;
	private static Map<String,Map<String,Integer>> mapMonthResult;
	private static final int MAX_RECOMMENDATIONS = 3;

	{
	    mapMonth=mapFromArrays(months, values);
	}
	
	public static <K,V> Map<K,V> mapFromArrays(K[] keys,V[]values){
	    HashMap<K, V> result=new HashMap<K, V>();
	    for(int i=0;i<keys.length;i++){
	        result.put(keys[i], values[i]);
	    }
	    return result;

	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			mapDateResult = new HashMap<String,Map<String,Integer>>();
			mapMonthResult = new HashMap<String,Map<String,Integer>>();
			getTweetsData();
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		calculateRatings();
		try {
			Set<RatingDTO> set = MongoConnection.getAllRatingsRecommendation();
			ItemRecommender irec = getRecommender(set);
			saveModel(irec, set);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (RecommenderBuildException e) {
			e.printStackTrace();
		}
	}
	
	
	public ArrayList<String> getTweetDate(String date) {
		ArrayList<String> result = new ArrayList<String>();
		String[] arrayDate = date.split(" ");
		String month = arrayDate[1];
		String year = arrayDate[5];
		String fullDate = arrayDate[2]+"/"+mapMonth.get(month)+"/"+year;
		result.add(month+year);
		result.add(fullDate);
		return result;
	}
	
	
	public void getTweetsData() throws JsonProcessingException {
		Iterable<Document> result = MongoConnection.getAlltweets();
		SortedSet<String> keywords = MongoConnection.getKeywords();
		try {
			for(Document doc:result) {
				if(doc==null || doc.get("created_at")==null || doc.get("text")==null) {
					continue;
				}
				String month = getTweetDate((String) doc.get("created_at")).get(0);
				String date = getTweetDate((String) doc.get("created_at")).get(1);
				String text = (String) doc.get("text");
				for(String keyword:keywords) {
					if(text.contains(keyword)) {
						Map<String,Integer> mapKeywordsDate;
						Map<String,Integer> mapKeywordsMonth;
						if(mapDateResult.containsKey(date)) {
							mapKeywordsDate = mapDateResult.get(date);
						}else{
							mapKeywordsDate = new HashMap<String,Integer>();
						}
						if(mapMonthResult.containsKey(month)){
							mapKeywordsMonth = mapMonthResult.get(month);
						}else{
							mapKeywordsMonth = new HashMap<String,Integer>();
						}
						if(mapKeywordsDate.containsKey(keyword)) {
							Integer i = mapKeywordsDate.get(keyword)+1;
							mapKeywordsDate.put(keyword, i);
						}else {
							mapKeywordsDate.put(keyword, 1);
						}
						if(mapKeywordsMonth.containsKey(keyword)) {
							Integer i = mapKeywordsMonth.get(keyword)+1;
							mapKeywordsMonth.put(keyword, i);
						}else {
							mapKeywordsMonth.put(keyword, 1);
						}
						mapDateResult.put(date, mapKeywordsDate);
						mapMonthResult.put(month, mapKeywordsMonth);
					}
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e);
		}
		
		
		for(String key:mapDateResult.keySet()) {
			Map<String,Map<String,Integer>> mapDateResultAux = new HashMap<String,Map<String,Integer>>();
			mapDateResultAux.put(key, mapDateResult.get(key));
			MongoConnection.saveTweetsData(mapDateResultAux,0);
		}
		for(String key:mapMonthResult.keySet()) {
			Map<String,Map<String,Integer>> mapMonthResultAux = new HashMap<String,Map<String,Integer>>();
			mapMonthResultAux.put(key, mapMonthResult.get(key));
			MongoConnection.saveTweetsData(mapMonthResultAux,1);
		}
		System.out.println("Hecho");
	}

	public List<String> keysConverter(String keys) {
		String[] result = keys.split(",");
		return Arrays.asList(result);
	}
	
	public void calculateRatings() {
		Iterable<Document> documents = MongoConnection.getAllBooks();
		List<Document> books = new ArrayList<>();
		List<RatingDTO> ratings = new ArrayList<>();
		documents.forEach(
			x -> books.add(x));
		for(Integer i = 0; i < books.size()-1; i++) {
			String isbn1 = (String) books.get(i).get("idBooks");
			String keys1 = (String) books.get(i).get("keywords");
			if(keys1!=null && keys1.trim()!="") {
				List<String> listKeys1 = keysConverter(keys1.trim());
				for(Integer a = 0; a<books.size()-1; a++) {
					if(i!=a) {
					String isbn2 = (String) books.get(a).get("idBooks");
					String keys2 = (String) books.get(a).get("keywords");
					if(keys2!=null && keys2.trim()!="") {
						List<String> listKeys2 = keysConverter(keys2.trim());
						Integer e = 0;
						for(String keyword:listKeys2) {
							if(listKeys1.contains(keyword)) {
									e = e + 1;
							}
						}
						if(e >= 1) {
							Integer rating = (int) Math.round((e*5)/listKeys1.size());
							String id = isbn1+isbn2;
							RatingDTO bookRating = new RatingDTO(isbn1,isbn2,rating,id);
							ratings.add(bookRating);
							//System.out.println("Hecho");
						}
				}
				}
			}
		}
		
	}
		MongoConnection.saveRatings(ratings);
	}
	
	public static ItemRecommender getRecommender(Set<RatingDTO> dtos) throws RecommenderBuildException {
		LenskitConfiguration config = new LenskitConfiguration();
		EventDAO myDAO = EventCollectionDAO.create(createEventCollection(dtos));

		config.bind(EventDAO.class).to(myDAO);
		config.bind(ItemScorer.class).to(UserUserItemScorer.class);

		Recommender rec = LenskitRecommender.build(config);
		return rec.getItemRecommender();
	}

	private static Collection<? extends Event> createEventCollection(Set<RatingDTO> ratings) {
		List<Event> result = new LinkedList<>();
		for (RatingDTO dto : ratings) {
			MutableRating r = new MutableRating();
			r.setItemId(dto.getIsbn1().hashCode());
			r.setUserId(dto.getIsbn2().hashCode());
			r.setRating(dto.getRating());
			result.add(r);
		}
		return result;
	}
	
	public static void saveModel(ItemRecommender irec, Set<RatingDTO> set) throws IOException {
		Map<String, Long> keys = Maps.asMap(set.stream().map((RatingDTO x) -> x.getIsbn1()).collect(Collectors.toSet()),
				(String y) -> new Long(y.hashCode()));
		Map<Long, String> reverse = new HashMap<>();
		for(String key:keys.keySet()) {
			reverse.put(keys.get(key), key);
		}
		
		for (String key : keys.keySet()) {
			List<ScoredId> recommendations = irec.recommend(keys.get(key), MAX_RECOMMENDATIONS);
			
			if (recommendations.size() > 0) {
				for(ScoredId rec:recommendations) {
					String isbn2 = reverse.get(rec.getId());
					if(!key.equals(isbn2)) {
						RatingDTO result = new RatingDTO(key,isbn2,(int) rec.getScore(),null);
						MongoConnection.saveRatingRecommendation(result);
					}
				}
				
			}
		}

	}
	
	
}

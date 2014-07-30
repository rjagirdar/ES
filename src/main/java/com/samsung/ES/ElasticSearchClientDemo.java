package com.samsung.ES;

import java.util.ArrayList;
import java.util.Arrays;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.samsung.ES.Model.Movie;

public class ElasticSearchClientDemo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Hello RJ");
		Index();
	}
	
	public static void Index(){
		try
		{
			TransportClient client=getClient();
			if(client!=null){
				System.out.println("Client Created");
			}
			else
				System.out.println("Client Creation Failed");
			
			ArrayList<Movie> movies=getMovies();
			for(Movie currMovie : movies){
				String json=getJsonString(currMovie);
				IndexResponse response= client.prepareIndex("pmovies", "pmovie")
						.setSource(json)
						.execute().actionGet();

				System.out.println("Index Name: "+response.getIndex());
				System.out.println("Index Type: "+response.getType());
				System.out.println("ID: "+response.getId());
				System.out.println("version: "+response.getVersion());

			}
		}
		catch(Exception ex){
			System.out.println(ex.getMessage());
		}

	}
	
	public static ArrayList<Movie> getMovies(){
		ArrayList<Movie> movies=new ArrayList<Movie>();
		Movie currMovie=new Movie();
		
		currMovie.setTitle("The Godfather");
		currMovie.setDirector("Francis Ford Coppola");
		currMovie.setYear(1972);
		currMovie.setGenres(Arrays.asList("Crime","Drama"));
		
		movies.add(currMovie);
		
		currMovie=new Movie();
		currMovie.setTitle("Lawrence of Arabia");
		currMovie.setDirector("David Lean");
		currMovie.setYear(1962);
		currMovie.setGenres(Arrays.asList("Adventure", "Biography", "Drama"));
		
		movies.add(currMovie);
		
		currMovie=new Movie();
		currMovie.setTitle("To Kill a Mockingbird");
		currMovie.setDirector("Robert Mulligan");
		currMovie.setYear(1962);
		currMovie.setGenres(Arrays.asList("Crime", "Drama", "Mystery"));
		
		movies.add(currMovie);
		
		currMovie=new Movie();
		currMovie.setTitle("Apocalypse Now");
		currMovie.setDirector("Francis Ford Coppola");
		currMovie.setYear(1979);
		currMovie.setGenres(Arrays.asList("Drama", "War"));
		
		movies.add(currMovie);
		
		currMovie=new Movie();
		currMovie.setTitle("Kill Bill: Vol. 1");
		currMovie.setDirector("Quentin Tarantino");
		currMovie.setYear(2003);
		currMovie.setGenres(Arrays.asList("Action", "Crime", "Thriller"));
		
		movies.add(currMovie);
		
		currMovie=new Movie();
		currMovie.setTitle("The Assassination of Jesse James by the Coward Robert Ford");
		currMovie.setDirector("Andrew Dominik");
		currMovie.setYear(2007);
		currMovie.setGenres(Arrays.asList("Biography", "Crime", "Drama"));
		
		movies.add(currMovie);
		return movies;
	}
	
	public static String getJsonString(Movie currMovie){
		ObjectMapper mapper=new ObjectMapper();
		String json="";
		try {
			json = mapper.writeValueAsString(currMovie);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return json;
	}
	
	public static String getJsonStringUsingXcontent(){
		String returnVal="";
		try{
		XContentBuilder builder=jsonBuilder().startObject()
								.field("title","The Godfather")
								.field("director", "Francis Ford Coppola")
								.field("year", 1972)
								.endObject();
		returnVal=builder.string();
		}
		catch(Exception ex){
			System.out.println(ex.getMessage());
		}
		return returnVal;
	}
	
	public static TransportClient getClient(){
		Settings settings=ImmutableSettings.settingsBuilder().put("elasticsearch", "localtestsearch").build();
		TransportClient client=new TransportClient(settings);
		client=client.addTransportAddress(new InetSocketTransportAddress("127.0.0.1", 9300));
		return client;
	}

}
/**
 * Reducer class is used to find Average Rating and the Top Movie as per Genre
 * @author Dhruv
 *
 */


package edu.tamu.isys.ratings;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MyReducer extends  Reducer<Text, IntWritable, Text, Text> {

 
	//Defining arrays to keep the max value of the ratings with movie and genre
	ArrayList<String> genreList = new ArrayList<String>();
	ArrayList<String> movieList = new ArrayList<String>();
	List<Double> ratingList = new ArrayList<Double>();
	
	private Text movie_rating = new Text();

	protected void setup(Context context) throws java.io.IOException,
			InterruptedException {

	}

	@Override
	public void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		// Finding the average rating from different users for a movie
		int sum = 0;
		double count = 0.0;
		for (IntWritable val : values) {
			sum += val.get();
			count++;
		}
		// Converting average rating to two place decimal
		double avg1 = sum / count;
		DecimalFormat newFormat = new DecimalFormat("#.##");
		double avg = Double.valueOf(newFormat.format(avg1));
		
		//Splitting the Key(Genre::MovieName) to individual values
		String movie = key.toString();
		String[] movie_genre = movie.split("::");
		String genre = movie_genre[0];

		//Finding maximum 
		if (genreList.contains(genre)) {
			int index = genreList.indexOf(genre);
			if (avg > ratingList.get(index)) {
				ratingList.set(index, avg);
				movieList.set(index, movie_genre[1]);

			}
		} else {
			genreList.add(genre);
			ratingList.add(avg);
			movieList.add(movie_genre[1]);

		}

	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		//Displaying the maximum ratings for a particular genre
		for (int i = 0; i < genreList.size(); i++) {
			movie_rating.set(movieList.get(i) + " ("
					+ Double.toString(ratingList.get(i)) + ")");
			context.write(new Text(genreList.get(i)),
					new Text(movie_rating));

		}

	}

}

/**
 * Mapper class is used to divide data by movies and genre
 * @author Dhruv
 *
 */

package edu.tamu.isys.ratings;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable rating = new IntWritable(1);
	private Text genre_movie = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//Spliting the input line into movies ,genre ,ratings
		String line = value.toString();
		String[] temp;
		temp = line.split("::");
		String[] temp2;
		temp2 = temp[2].split(",");
		String movie = temp[1];

		rating.set(Integer.parseInt(temp[6]));
		// writing outputs for according to genre
		for (int i = 0; i < temp2.length; i++) {

			genre_movie.set(temp2[i] + "::" + movie);

			context.write(genre_movie, rating);

		}
	}

}
package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffRed2 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double diff_max = 0.0; // sets diff_max to a default value
		/* 
		 * TODO: finish
		 * Compute and emit the maximum of the differences
		 */
		
		
		double diff = 0.0;
		
		for (Text value: values){
			System.out.println("DiffRed2: value ============ " + value.toString());
			diff  = Double.parseDouble(value.toString());
			if( diff > diff_max){
				diff_max = diff;
			}	
		}
		
		context.write(new Text(""), new Text(String.valueOf(diff_max)));
		
		
	}
}

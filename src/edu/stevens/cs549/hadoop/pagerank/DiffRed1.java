package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffRed1 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double[] ranks = new double[2];
		/* 
		 * TODO: finish
		 * The list of values should contain two ranks.  Compute and output their difference.
		 */
		
		
		/*
		 * 	input:
		 * 		key: node
		 * 		values: {rank1, rank2}
		 * 
		 * output:
		 * 		key: does not matter
		 * 		value: abs( rank1 - rank2)
		 * 
		 * */
		
		System.out.println("DiffRed1: key ============ " + key.toString());
		
		double difference = 0.0;
		
		int i = 0;
		
		
		for(Text value: values){
			
			System.out.println("DiffRed1: value ============ " + value.toString());
				
			if( i == 0){
				
				ranks[0] = Double.parseDouble( value.toString() );
				i++;
			}
			else{
				
				ranks[1] = Double.parseDouble( value.toString() );
			}

			
			
		}
		difference = Math.abs( ranks[1]-ranks[0] );
		context.write(key, new Text(String.valueOf(difference)));

	}
}

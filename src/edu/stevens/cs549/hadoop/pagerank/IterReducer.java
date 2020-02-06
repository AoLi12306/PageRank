package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterReducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double d = PageRankDriver.DECAY; // Decay factor
		/* 
		 * TODO: finish
		 * emit key:node+rank, value: adjacency list
		 * Use PageRank algorithm to compute rank from weights contributed by incoming edges.
		 * Remember that one of the values will be marked as the adjacency list for the node.
		 */
		
		System.out.println("IterReducer: key_input ============ " + key.toString());
		
		
		/*
		 * 	input: 
		 * 		case1:
		 * 			key: node
		 * 			value: ranks 		all vote points node got
		 * 		case2:
		 * 			value: node [Tab] -adjcent list
		 * 
		 * 
		 * 	output:			This is the same format as IterMapper
		 * 		key: node+rank
		 * 		value: adjacency list
		 * */
		
		String key_out = key.toString();
		key_out += "+";
		String value_out = "";
		double rank = 0.0;

		
		for(Text value: values){
			System.out.println("IterReducer: values ============ " + value.toString());
			
			
			if( value.toString().contains("-") ){
				/*
				 * 	-abc
				 *   	will split to
				 *   {"","abc"}
				 * 	so [1], not [0]
				 * */
				value_out = value.toString().split("-")[1];
			}
			else{
				rank += Double.parseDouble(value.toString().trim());
				
			}
		}
		
		rank += (1-d);
		
		key_out += String.valueOf(rank);
		
		
		
		
		
		context.write(new Text(key_out), new Text(value_out));
		
		
		
		
	}
}

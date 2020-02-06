package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		
		/* 
		 * TODO: finish
		 * Just echo the input, since it is already in adjacency list format.
		 */
		
		
	
		
	
		
		/*
		 *	input: 
		 *		key-in:
		 *		value-in:	node-id: to-node1 to-node2 ... 
		 * 
		 * output:
		 * 		key-out: 	node-id
		 * 		value-out:	to-node1 to-node2 ...
		 * 
		 */
		
		
		
		String result[] = line.split(":");
		
		String keyout = result[0];
		String valout = "";
		if( result.length == 2){
			valout = result[1];
		}
		
		System.out.print (keyout);
		
		context.write(new Text(keyout), new Text(valout) );


	}

}

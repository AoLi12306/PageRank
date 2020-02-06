package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/* 
		 * TODO: finish
		 * Output key: node+rank, value: adjacency list
		 */
		
		
		/*
		 * 	input:
		 * 		key: node
		 * 		values:		{	{node1, node2, ...},  {node1,node2,...} ...    }
		 * 
		 * output:
		 * 		key: node+1.0
		 * 		value:		{ node1, node2, ... , node1, node2, ...,  ...}
		 * 
		 * */
		
		String out_key;
		String out_value = "";
		
		out_key = key.toString();
		
		out_key = out_key + "+1.0";
		
		boolean isHead = true;
		
		
		for(Text value: values){
			if(isHead){
				out_value = value.toString();
				isHead = false;
			}
			else{
				out_value += " " + value.toString(); 
			}
		}
		
		
		context.write(new Text(out_key), new Text(out_value) );
		

	}
}

package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffMap1 extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		String[] sections = line.split("\t"); // Splits each line
		if (sections.length > 2) // checks for incorrect data format
		{
			throw new IOException("Incorrect data format");
		}
		/**
		 *  TODO: finish
		 *  read node-rank pair and emit: key:node, value:rank
		 */
		
		/*
		 * 	input:
		 * 		value: node+rank [Tab] adj_list
		 * 
		 * 	output:
		 * 		key: node
		 * 		value: rank
		 * */
		
		String node_and_rank[] = sections[0].split("\\+");
		
		if (node_and_rank.length == 2){
		
			String node = node_and_rank[0];
			String rank = node_and_rank[1];
			
			context.write(new Text(node),new Text(rank) );
		}
		else{
			
			throw new IOException(" ============== Incorrect data format£º node+rank ==============");
			
		}

	}

}

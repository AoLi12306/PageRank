package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class FinMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		/*
		 * TODO finish
		 * output key:-rank, value: node
		 * See IterMapper for hints on parsing the output of IterReducer.
		 */
		
		
		
		/*
		 * input:
		 * 		key:	nobody care
		 * 		value:	node+rank adjcent list 
		 * 
		 * output:
		 * 		key: -rank			this is because we need reverse rank
		 * 		value: node
		 * */
		
		String sections[] = line.split("\t");
		String node_and_rank[] = sections[0].split("\\+");
		String node = node_and_rank[0];
		double rank = Double.parseDouble(node_and_rank[1]);
		rank = 0-rank;
		
		
		String value_out = node;
		
		context.write(new DoubleWritable(rank), new Text(value_out));

	}

}

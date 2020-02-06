package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffMap2 extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String s = value.toString(); // Converts Line to a String

		/* 
		 * TODO: finish
		 * emit: key:"Difference" value:difference calculated in DiffRed1
		 */
		
		
		/*
		 * 	input:		this input is read form tempdiff/part-r-00000
		 * 		key: not matter
		 * 		values: node [Tab] difference
		 * 
		 * output:
		 * 		key: "Difference"
		 * 		value: difference
		 * 
		 * */
		
		String sections[] = s.split("\\s+");
	
		if( sections.length == 2){
			
			
			
			context.write(new Text("Difference"), new Text(sections[1]) );
		}
		else {
			
			throw new IOException(" ==============DiffMap2£º Incorrect data format£º node difference ==============");
		}
		
		
		
	}

}

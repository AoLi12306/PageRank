package edu.stevens.cs549.hadoop.pagerank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class FinReducer extends Reducer<DoubleWritable, Text, Text, Text> {
	
	HashMap<Long, String> maps = new HashMap<Long, String>(); 
	int size = 0;

	
	
	protected void setup(Context context) throws IOException, InterruptedException {	
		URI[] files = context.getCacheFiles();
		  
		if (files.length > 0){
			Path path = new Path(files[0]);
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(files[0], conf);
		  
			if ( fs.exists(path) )
			{
				FSDataInputStream in = fs.open(path);				  
				BufferedReader d = new BufferedReader(new InputStreamReader(in));				  
				
				String diffcontent = "x"; // Initializes string to random value
				while ( diffcontent != null) 
				{
					diffcontent = d.readLine();
					System.out.println(" diffcontent ============= " + diffcontent);
					if (diffcontent != null) // Adds new value as long as value exists
					{
						String[] parts = diffcontent.trim().split(" ");
						System.out.println(" parts[0] ============= " + parts[0]);
						if(parts.length == 2){
							
							System.out.println(" parts[1] ============= " + parts[1]);
							Long node_id = Long.parseLong(parts[0]);
							String node_name = parts[1];
							maps.put(node_id, node_name);					
							size++; // Counter to calculate size of HashMap}
						}
						else{
							
							throw new IOException("Incorrect data format£º node_id node_name");
						}
						
					}
				}
				d.close();				   				  				  				  
				in.close();
			}
		}
		
		System.out.println(" =============       =====================         =======================");
		
		Iterator<Entry<Long, String>> it = maps.entrySet().iterator();
		while (it.hasNext()){
	        Map.Entry pair = (Map.Entry)it.next();
	        System.out.println(pair.getKey() + " = " + pair.getValue());
		}	
		
		
	}
	


	
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {
		/* 
		 * TODO: finish 
		 * For each value, emit: key:value, value:-rank
		 */

		
		double rank = 0 - Double.parseDouble(key.toString());
		
		for(Text value: values){
			if(maps.isEmpty()){
				
				 System.out.println("The node id to page name map is empty!");
				
				context.write(value, new Text( String.valueOf(rank) ) );
			}
			else{
				Long node_id = Long.parseLong(value.toString());
				String node_name = maps.get(node_id);
				
				
				System.out.println("The node id to page name map is " + node_name );
				
				context.write(new Text(node_name), new Text( String.valueOf(rank) ) );
				
				
			}
		}

	}
}

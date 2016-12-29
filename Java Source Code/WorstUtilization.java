import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WorstUtilization {
	public static class Mapper1 extends Mapper<Object, Text, Text, Text>{

		private Text textObject = new Text();
		private Text result = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] fields=value.toString().split(","); 
			if(fields[1].equals("Unknown")||fields[2].equals("Unknown")||fields[2].equals("Arr Arr")||!StringUtils.isNumeric(fields[7])||!StringUtils.isNumeric(fields[8]))return;
			textObject.set(fields[1].split(" ")[1].toString()+"_"+fields[2].split(" ")[0].toString());
			result.set(Integer.toString(Integer.parseInt(fields[8])-Integer.parseInt(fields[7])));
			context.write(textObject,result);
		}
	}
	
	public static class Reducer1 extends Reducer<Text,IntWritable,Text,Text> {
		private IntWritable reducer1Output = new IntWritable();
		Text result = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum=0;
			for(Text value:values){
				sum+=Integer.parseInt(value.toString());
			}
			reducer1Output.set(sum);
			result.set(reducer1Output.toString());
			context.write(key,result);
		}
	}
	
	public static class Mapper2 extends Mapper<Object, Text, Text, Text>{
	  	Text textObject=new Text();
	  	Text valueObject = new Text();
	  	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	  		String[] fields=value.toString().split("\\t"); 
	  		String key1=fields[0].split("_")[0].toString();
	  		textObject.set(key1);
	  		valueObject.set(fields[0].split("_")[1].toString()+"_"+fields[1].toString());
	  		context.write(textObject, valueObject);
	  		
	  	}
	}
	
	public static class Reducer2 extends Reducer<Text,Text,Text,IntWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Iterator<Text> iterator=values.iterator();
			HashMap<String, Integer> valueMap = new HashMap<String, Integer>();
			while(iterator.hasNext()){
				String value1 = iterator.next().toString();
				valueMap.put(value1.split("_")[0], Integer.parseInt(value1.split("_")[1]));
			}
			Entry<String, Integer> max = Collections.max(valueMap.entrySet(), new Comparator<Entry<String,Integer>>(){
				public int compare(Entry<String, Integer> entry1, Entry<String, Integer> entry2) {
			        return entry1.getValue().compareTo(entry2.getValue());
				}    
			});
			key.set(key + "_" + max.getKey());
			int reducerOutput = Integer.parseInt(max.getValue().toString());
			context.write(key,new IntWritable(reducerOutput));
		}
	}

	public static void main(String[] args) throws Exception {
		String temp="WorstUtilization";
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "get Capacity minus enrollment difference");
	    job.setJarByClass(WorstUtilization.class);
	    job.setMapperClass(Mapper1.class);
	    job.setCombinerClass(Reducer1.class);
	    job.setReducerClass(Reducer1.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(temp));
	    job.waitForCompletion(true);
	    Configuration conf2 = new Configuration();
	    Job job2 = Job.getInstance(conf2, "get worst utilization/max difference for each year");
	    job2.setJarByClass(WorstUtilization.class);
	    job2.setMapperClass(Mapper2.class);
	    job2.setReducerClass(Reducer2.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job2, new Path("WorstUtilization"));
	    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
	    System.exit(job2.waitForCompletion(true) ? 0 : 1);
	    
	  }


}
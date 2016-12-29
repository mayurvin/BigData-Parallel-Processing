import java.io.IOException;
import java.util.Iterator;

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

public class EnrollmentsInENG {
	public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable>{

		private Text textObject = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] fields=value.toString().split(","); 
			if(!fields[4].equals("ENG")||fields[3].equals("Unknown")||!StringUtils.isNumeric(fields[9]))return;
			textObject.set(fields[3].split(" ")[1].toString()+" "+fields[4].toString());
			context.write(textObject,new IntWritable(Integer.parseInt(fields[9])));
		}
	}
	
	public static class Reducer1 extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable reducer1Output = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum=0;
			for(IntWritable value:values){
				sum+=value.get();
			}
			reducer1Output.set(sum);
			context.write(key,reducer1Output);
		}
	}
	
	public static class Mapper2 extends Mapper<Object, Text, Text, IntWritable>{
	  	Text textObject=new Text();

	  	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	  		String[] fields=value.toString().split("\\t"); 
	  		String key1=fields[0].split(" ")[0].toString()+"-"+Integer.toString(Integer.parseInt(fields[0].split(" ")[0].toString())+1) + " " + fields[0].split(" ")[1].toString();
	  		String key2=Integer.toString(Integer.parseInt(fields[0].split(" ")[0].toString())-1)+"-"+fields[0].split(" ")[0].toString() + " " + fields[0].split(" ")[1].toString();
	  		textObject.set(key1);
	  		context.write(textObject,new IntWritable(Integer.parseInt(fields[1])));
	  		textObject.set(key2);
	  		context.write(textObject,new IntWritable(Integer.parseInt(fields[1])));
	  	}
	}
	
	public static class Reducer2 extends Reducer<Text,IntWritable,Text,IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			Iterator<IntWritable> iterator=values.iterator();
			int prevYear=iterator.next().get();
			if(!iterator.hasNext())return;
			int CurrentYear=iterator.next().get();
			int reducerOutput = CurrentYear-prevYear;
			context.write(key,new IntWritable(reducerOutput));
		}
	}

	public static void main(String[] args) throws Exception {
		String temp="EnrollmentsEng";
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "get Enrollments for Engineering for every Year");
	    job.setJarByClass(EnrollmentsInENG.class);
	    job.setMapperClass(Mapper1.class);
	    job.setCombinerClass(Reducer1.class);
	    job.setReducerClass(Reducer1.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(temp));
	    job.waitForCompletion(true);
	    Configuration conf2 = new Configuration();
	    Job job2 = Job.getInstance(conf2, "get change in Enrollments between every consecutive years");
	    job2.setJarByClass(EnrollmentsInENG.class);
	    job2.setMapperClass(Mapper2.class);
	    job2.setReducerClass(Reducer2.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job2, new Path("EnrollmentsEng"));
	    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
	    System.exit(job2.waitForCompletion(true) ? 0 : 1);
	    
	  }

}
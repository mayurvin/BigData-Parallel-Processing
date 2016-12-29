import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SpringFallYearWiseCourseEnrollment {

	public static class Mapper1
	extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		String unknown = "unknown";
		String arr = "arr";

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			List<String> tokenList = Arrays.asList(value.toString().split(","));
			String hallName = null;
			if(tokenList.get(2).indexOf(" ") != -1){

				hallName = tokenList.get(2).substring(0, tokenList.get(2).indexOf(" "));
			}
			else{
				hallName = tokenList.get(2);
			}
			if(!unknown.equalsIgnoreCase(hallName.toLowerCase()) && !arr.equalsIgnoreCase(hallName.toLowerCase())){
				if(isInteger(tokenList.get(7).toString())){
					if(Integer.parseInt(tokenList.get(7).toString()) >= 0){ 
						word.set(tokenList.get(6) + "_" + tokenList.get(1));
				    	context.write(word,new IntWritable(Integer.parseInt(tokenList.get(7))));		
					}
				}
			}

		}
	}

	public static class Reducer1
	extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			int sum=0;
			for(IntWritable val:values){
				sum+=val.get();
			}
			result.set(sum);
			context.write(key,result);
		}
	}
	
	public static class Mapper2 extends Mapper<Object, Text, Text, Text>{
	  	Text textObject=new Text();
	  	Text valueObject = null;
	  	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	  		String[] fields=value.toString().split("_"); 
	  		String key1 = fields[0];
	  		String[] valueArray = fields[1].split(" ");
	  		key1 = key1 + "_" + valueArray[1].split("\\t")[0];
	  		textObject.set(key1);
	  		valueObject = new Text(valueArray[0] + "_" + valueArray[1].split("\\t")[1]);
	  		context.write(textObject,valueObject);
	  	}
	}
	
	public static class Reducer2 extends Reducer<Text,Text,Text,IntWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Iterator<Text> iterator=values.iterator();
			int springValue = 0;
			int fallValue = 0;
			String key1 = key + "_Spring-Fall";
			String value = iterator.next().toString();
			String semester = value.split("_")[0];
			if(semester.equalsIgnoreCase("Spring"))
				springValue=Integer.parseInt(value.split("_")[1]);	
			else if(semester.equalsIgnoreCase("Fall"))
				fallValue=Integer.parseInt(value.split("_")[1]);
			
			if(!iterator.hasNext())return;
			value = iterator.next().toString();
			semester = value.split("_")[0];
			if(semester.equalsIgnoreCase("Spring"))
				springValue=Integer.parseInt(value.split("_")[1]);
			else if(semester.equalsIgnoreCase("Fall"))
				fallValue=Integer.parseInt(value.split("_")[1]);	
			int reducerOutput = springValue-fallValue;
			context.write(new Text(key1),new IntWritable(reducerOutput));
		}
	}

	public static boolean isInteger( String input )
	{
		try
		{
			Integer.parseInt( input );
			return true;
		}
		catch( Exception e )
		{
			return false;
		}
	}
	public static void main(String[] args) throws Exception {
		String temp="CourseEnrollment";
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Semester wise Course enrollments");
		job.setJarByClass(SpringFallYearWiseCourseEnrollment.class);
		job.setMapperClass(Mapper1.class);
		job.setCombinerClass(Reducer1.class);
		job.setReducerClass(Reducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(temp));
		job.waitForCompletion(true);
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Year wise difference of Spring and Fall enrollments");
		job2.setJarByClass(SpringFallYearWiseCourseEnrollment.class);
		job2.setMapperClass(Mapper2.class);
		//job2.setCombinerClass(FindIncreaseReducer.class);
		job2.setReducerClass(Reducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path("CourseEnrollment"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);

	}
}
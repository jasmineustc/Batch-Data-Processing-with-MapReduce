import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


/**
 * 
 * @author zhangmi
 *
 */
public class LanguageWordCount {
	
	public static class Query implements Comparable<Query>{
		
		private String word;
		private int count;
		
		public Query(String word, int count){
			this.word = word;
			this.count = count;
		}

		@Override
		public int compareTo(Query o) {
			if(o.count != count){
				// need to sort from high to low
				return o.count - count;
			}else{
				return word.compareTo(o.word);
			}
		}
	}
	
	
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, Text> {
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();			
			int index = line.lastIndexOf(" ");	
			
			// this line only contains one word, just ignore it.
			if(index==-1){
				return;
			}
			
			String word = line.substring(0,index);
			// because hadoop format if <phase>\t<count>, need to replace
			// the \t to other char
			String phase = line.replaceAll("\t", ":");
					
			Text w = new Text(word.trim());
			Text p = new Text(phase.trim());
			context.write(w, p);
		}
	}
	
	
	
	public static class IntSumReducer extends
			TableReducer<Text, Text, ImmutableBytesWritable> {	
		private Configuration conf;
		private int n;
		
		
		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			conf = context.getConfiguration();
			// we need to get the first n phase
			n = conf.getInt("n", 5);
		}
		

		public void reduce(Text key,Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			PriorityQueue<Query> myQueue = new PriorityQueue<Query>();
			
			for(Text v: values){
				String[] vals=v.toString().split(":");				
				int index = vals[0].lastIndexOf(" ");
				String word = vals[0].substring(index+1);
				int count = Integer.parseInt(vals[1]);
				myQueue.add(new Query(word, count));
			}
			//put the row key into table
			Put put = new Put(Bytes.toBytes(key.toString()));
			int count = 0;
			while (myQueue.peek() != null && count < 5) {
				Query q = myQueue.poll();
				String wordCount = String.valueOf(q.count);
				// add word to column - family "data"
				put.add(Bytes.toBytes("data"), Bytes.toBytes(q.word), Bytes.toBytes(wordCount));
				count += 1;
			}
			context.write(null, put);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();
		
		
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(LanguageWordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// table name in habse is wordcount
		TableMapReduceUtil.initTableReducerJob("wordcount", IntSumReducer.class, job);
		
		List<String> otherArgs = new ArrayList<String>();
		for (int i = 0; i < remainingArgs.length; ++i) {
			otherArgs.add(remainingArgs[i]);
		}
		
		FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		
	}

}

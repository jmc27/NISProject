import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.*;

import javax.security.auth.login.Configuration;

import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;



public class Query {

	public static class QueryMapper extends MapReduceBase
	implements Mapper<LongWritable, Text, Text, Text> {
	
	private Text currentWord = new Text();
	private String query;
	
		public void configure(JobConf conf){
			query = conf.get("parameter");
		}
	
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text>	output, Reporter report)
				throws IOException {
			
			StringTokenizer parse = new StringTokenizer(query);
			HashSet<String> include = new HashSet<String>();
			HashSet<String> exclude = new HashSet<String>();
			
			boolean check = true;
			while(parse.hasMoreTokens()){
				String current = parse.nextToken();
				if (current.equals("not")){
					check = false;
				}
				if (check){
					include.add(current);
				} else {
					exclude.add(current);
				}
			}
			
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
      		String word = tokenizer.nextToken().toLowerCase();
      		if (include.contains(word)){
      			currentWord.set(word);
      			while (tokenizer.hasMoreTokens()){
      				output.collect(new Text(tokenizer.nextToken()), currentWord);
      			}
      		} 
      		if (exclude.contains(word)){
      			currentWord.set("not "+word);
      			while (tokenizer.hasMoreTokens()){
      				output.collect(new Text(tokenizer.nextToken()), currentWord);
      			}
      		}
			
		}
	}

	public static class QueryReducer extends MapReduceBase
		implements Reducer<Text, Text, Text, Text> {
		
		private String query;
		public void configure(JobConf conf){
			query = conf.get("parameter");
		}
		
		public void reduce (Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter report)
				throws IOException {
			boolean check = true;
			Set<String> wordSet = new HashSet<String>();
			
			while(values.hasNext() && check){
				String current = values.next().toString();
				wordSet.add(current);
				if (current.startsWith("not ")){
					check = false;
				}
				
			}
			
			if (check){
				String wordList = "";
				for (String word: wordSet){
					wordList = wordList + word +", ";
				}
			output.collect(key, new Text(wordList));
			}
		}
	}

	public static void main (String[] args) throws Exception {
		JobConf conf = new JobConf(Query.class);
		conf.setJobName("InvertedIndex");
		
		System.out.print("Enter query: ");
		Scanner scanner = new Scanner(System.in);
		conf.set("parameter", scanner.nextLine());
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setMapperClass(QueryMapper.class);
		conf.setReducerClass(QueryReducer.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		FileSystem.get(conf).delete(new Path("query_out"));
		long startTime = System.currentTimeMillis();
		JobClient.runJob(conf);
		System.out.println("Job finished in :" + (System.currentTimeMillis() - startTime) / 1000 + " seconds");
	}
}
import java.io.IOException;
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
			
			while(parse.hasMoreTokens()){
				String current = parse.nextToken();
				if (!( (current.equalsIgnoreCase("not")) || (current.equalsIgnoreCase("and")) || (current.equalsIgnoreCase("or")))){
					include.add(current);
				}
			}
			
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
      		String word = tokenizer.nextToken();
      		if (include.contains(word)){
      			currentWord.set(word);
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
			HashSet<String> words = new HashSet<String>();
			
			while(values.hasNext()){
				words.add(values.next().toString());
			}
			
			if (check(query, words)){
				output.collect(key,  new Text("1"));
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
	
	public static boolean check(String query, HashSet<String> words){
		Stack<String> stack = new Stack<String>();
		StringTokenizer tokenizer = new StringTokenizer(query);
		while (tokenizer.hasMoreTokens()){
			String current = tokenizer.nextToken().toString();
			if (current.equalsIgnoreCase("(")){
				stack.push(current);
			} else if (current.equalsIgnoreCase("not")){
				String word = stack.pop();
				if (word.equalsIgnoreCase("TRUE")){
					stack.push("FALSE");
				} else if (word.equalsIgnoreCase("FALSE")){
					stack.push("TRUE");
				} else {
					if (words.contains(word)){
						stack.push("FALSE");
					} else {
						stack.push("TRUE");
					}
				}
				stack.pop();
			} else if (current.equalsIgnoreCase("or")){
				ArrayList<String> list = new ArrayList<String>();
				String pop = "";
				while (!pop.equalsIgnoreCase("(")){
					pop = stack.pop();
					if (!pop.equalsIgnoreCase("(")){
						list.add(pop);
					}
				}
				boolean check = false;
				for (String word: list){
					if (word.equalsIgnoreCase("TRUE")){
						check = true;
						break;
					} else if (words.contains(word)){
						check = true;
					}
				}
				if (check){
					stack.push("TRUE");
				} else {
					stack.push("FALSE");
				}
			} else if (current.equalsIgnoreCase("and")){
				ArrayList<String> list = new ArrayList<String>();
				String pop = "";
				while (!pop.equalsIgnoreCase("(")){
					pop = stack.pop();
					if (!pop.equalsIgnoreCase("(")){
						list.add(pop);
					}
				}
				boolean check = true;
				for (String word: list){
					if (!words.contains(word)){
						check = false;
						break;
					} else if (word.equalsIgnoreCase("FALSE")){
						check = false;
					}
				}
				if (check){
					stack.push("TRUE");
				} else {
					stack.push("FALSE");
				}
			} else if (current.equalsIgnoreCase(")")){
				
			} else {
				stack.push(current);
			}
		}	
		return stack.pop().equalsIgnoreCase("TRUE");
	}
}
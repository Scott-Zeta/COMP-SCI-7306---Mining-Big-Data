package word_count;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.ArrayList;

public class WordCount extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new WordCount(), args);

		System.exit(res);
	}
//build a new arraylist to check if the word is duplicate or not.
	public static ArrayList<String> keyList = new ArrayList<>();

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job job = new Job(getConf(), "WordCount");
		job.setJarByClass(WordCount.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// reads byte offset as key and whole line as value
		job.setInputFormatClass(TextInputFormat.class);
		// writes <k, v> pair per line
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		return 0;
	}

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable ONE = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
/*here is what was changed. subsitude a regular expression which means trun all non-alphabet char into a space, 
 * then split by any number of space. As the definition of the word must be composed by alphabet letter*/
			for (String token : value.toString().replaceAll("[^a-zA-Z]", " ")
					.toLowerCase().split("\\s+")) {
/*in the part, use the arraylist that define before. When the word is not in arraylist, do map progress as usual,
 * and then add the token into the arraylist. Once it detect the token has already in the list, do not count the duplicate word*/				
				if (!keyList.contains(token)) {
					keyList.add(token);
					//set the word, the hash key as the splited token's length 
					word.set(String.valueOf(token.length()));
					context.write(word, ONE);
				}
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;

			for (IntWritable val : values) {
				sum += val.get();
			}

			context.write(key, new IntWritable(sum));

		}
	}
}

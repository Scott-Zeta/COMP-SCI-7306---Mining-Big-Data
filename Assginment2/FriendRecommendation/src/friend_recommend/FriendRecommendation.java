package friend_recommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.ArrayList;

//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//import java.util.Arrays;
//import java.util.PriorityQueue;
//import java.util.Comparator;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map.Entry;


public class FriendRecommendation extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new FriendRecommendation(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "FriendRecommendation");
      job.setJarByClass(FriendRecommendation.class);
      
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(FriendCountWritable.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(KeyValueTextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
   //build a new Writable variable for job, with a user variable and a is/not friend condition variable.
   static public class FriendCountWritable implements Writable{
	   public Text user;
	   public IntWritable mutualFriend;
	   
	   //there are two numbers combination as the variable, a user and its potential mutual friend, default set as empty.
	   public FriendCountWritable(Text user, IntWritable mutualFriend){
		   this.user = user;
		   this.mutualFriend = mutualFriend;
	   }
	   
	   public FriendCountWritable(){
		   this.user = new Text();
		   this.mutualFriend = new IntWritable();
	   }
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.user.readFields(in);
		this.mutualFriend.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		this.user.write(out);
		this.mutualFriend.write(out);
	}

	public String getUser() {
		return user.toString();
	}

	public int getMutualFriend() {
		return mutualFriend.get();
	}
	
   }
   
   public static class Map extends Mapper<Text, Text, Text, FriendCountWritable> {
	   //	Mapper<keyin, value in, key out, value out> 
      @Override
      public void map(Text key, Text value, Context context)
              throws IOException, InterruptedException {
    	  ArrayList <String> friendsList = new ArrayList <String>();
    	  if (value.getLength() != 0){
    		  for (String friend: value.toString().split(",")){
    			  friendsList.add(friend);
    			  context.write(key, new FriendCountWritable(new Text(friend), new IntWritable(0)));
    			  //generate hash between user and its current friends, because they are already friends, set 0 for potential mutual friend.
    		  }
    		  
    		  for(int i = 0; i < friendsList.size(); i++ ){
    			  for(int j = i+1; j < friendsList.size(); j++){
    				  context.write(new Text(friendsList.get(i)), new FriendCountWritable(new Text(friendsList.get(j)), new IntWritable(1)));
    				  context.write(new Text(friendsList.get(j)), new FriendCountWritable(new Text(friendsList.get(i)), new IntWritable(1)));
    				  //generate hash between user's current friend each other, set the mutual friend as user itself.
    			  }
    		  }
    	  }
      }
   }

   public static class Reduce extends Reducer<Text, FriendCountWritable, Text, Text> {
	   //Reducer<keyin, value in, key out, value out>
      @Override
      public void reduce(Text key, Iterable<FriendCountWritable> values, Context context)
              throws IOException, InterruptedException {
         HashMap<String, Integer> mutualFriendsMap = new HashMap<String, Integer>();
         
         for(FriendCountWritable friendpairs: values){
        	 IntWritable mutualFriend = friendpairs.mutualFriend;
        	 String targetUser = friendpairs.user.toString();
        	 //if they are already friends, sign -1
        	 if(mutualFriend.get() == 0){
        		 mutualFriendsMap.put(targetUser, -1);
        	 }else{
        		 if(mutualFriendsMap.containsKey(targetUser)){
        			 //do not recommend If they already are friends
        			 if(mutualFriendsMap.get(targetUser) != -1){
        				 //if already has one or more friends
        				 mutualFriendsMap.put(targetUser, mutualFriendsMap.get(targetUser) + 1);
        			 }
        		 }else{
        			 //if has not been recommended, build a new hash in the map
        			 mutualFriendsMap.put(targetUser, 1);
        		 }
        	 }
         }
	        // use priorityqueue with comparator to sort decreasing for number of friends, increasing for ID number.
	        PriorityQueue<Entry<String, Integer>> ten_most_mutual = new PriorityQueue <Entry<String, Integer>>(10, new Comparator<Entry<String, Integer>>() {
	        	@Override
	            public int compare(Entry<String, Integer> user1, Entry<String, Integer> user2) {
	        		if (user2.getValue() == user1.getValue()){
	        			return Integer.parseInt(user1.getKey()) - Integer.parseInt(user2.getKey());
	        		}else{	
	        		return user2.getValue() - user1.getValue();
	        		}
	            }
	        });
	        
	        // eliminate all -1 with pairs that are already friends
	        for (Entry<String, Integer> pairs: mutualFriendsMap.entrySet()) {
	        	if (!pairs.getValue().equals(-1)) {
	        		ten_most_mutual.add(pairs);
	            }
         }
	        
	        // create a variable to store the top 10 friend recommendation result
	        StringBuilder result = new StringBuilder();
	        int number = 0;
	        int recommendation_number = ten_most_mutual.size();
	         
	        while (ten_most_mutual.isEmpty() == false) {
	        	// use string builder to print the result
	        	result.append(ten_most_mutual.poll().getKey());
	        	
	        	// only output the top 10 recommendations
	        	if (number >= 9 || number >= recommendation_number - 1) {
	            	break;
	            }

	            number++;
	            result.append(",");
	         }
	         
	        context.write(key, new Text(result.toString()));
      }
   }
}


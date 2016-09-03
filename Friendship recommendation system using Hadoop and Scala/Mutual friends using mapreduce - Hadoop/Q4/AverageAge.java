 import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class AverageAge 
{
    static int i=0;    
    public static class UserMapper extends Mapper<LongWritable, Text, LongWritable, Text> 
    {
           LongWritable user =new LongWritable();
            Text friends=new Text();

        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
             String line[] = value.toString().split("\t");
                user.set(Long.parseLong(line[0]));
                Text data=new Text();
                data.set(user.toString());
              
                if (line.length != 1)
                {
                    String mutualfriends = line[1];
                    String outvalue=("U:" + mutualfriends.toString());
                    context.write(user, new Text(outvalue));
                }
            }
        }
    
    
    public static class RatingsMapper extends Mapper<LongWritable, Text, LongWritable, Text> 
    {
        private LongWritable outkey = new LongWritable();
        private Text outvalue = new Text();
        @SuppressWarnings("deprecation")
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
             String arr[] = value.toString().split(",");
                if(arr.length == 10){
                    
                outkey.set(Long.parseLong(arr[0]));
                // Flag this record for the reducer and then output
                String[] cal=arr[9].toString().split("/");
                System.out.println("Ratings");
                 Date now = new Date();
                    int nowMonth = now.getMonth()+1;
                    int nowYear = now.getYear()+1900;
                    int result = nowYear - Integer.parseInt(cal[2]);

                    if (Integer.parseInt(cal[0]) > nowMonth) {
                        result--;
                    }
                    else if (Integer.parseInt(cal[0]) == nowMonth) {
                        int nowDay = now.getDate();

                        if (Integer.parseInt(cal[1]) > nowDay) {
                            result--;
                        }
                    }
                    String data=arr[1]+","+new Integer(result).toString()+","+arr[3]+","+arr[4]+","+arr[5];
                outvalue.set("R:" + data);
                        
                context.write(outkey, outvalue);
            }
        }

    }
    
    public static class UserRatingsReducer extends Reducer<LongWritable, Text, Text, Text> 
    {
        private ArrayList<Text> listA = new ArrayList<Text>();
        private ArrayList<Text> listB = new ArrayList<Text>();
        HashMap<String, String> myMap=new HashMap<String, String>();
         public void setup(Context context) throws IOException {
                Configuration config = context.getConfiguration();
                          myMap = new HashMap<String,String>();
                 String mybusinessdataPath = config.get("businessdata");
                 
                 Path pt=new Path(/*"hdfs://cshadoop1"+*/mybusinessdataPath);//Location of file in HDFS
                FileSystem fs = FileSystem.get(config);
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line;
                line=br.readLine();
                while (line != null){
                    String[] arr=line.split(",");
                    if(arr.length == 10){
                    myMap.put(arr[0].trim(), arr[1]+":"+arr[3]+":"+arr[9]); 
                    }
                    line=br.readLine();
                }     
            }
        @SuppressWarnings("deprecation")
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            // Clear our lists
            listA.clear();
            listB.clear();
            
            for (Text val : values)
            {
                if (val.toString().charAt(0) == 'U')
                {
                    listA.add(new Text(val.toString().substring(2)));
                }         
                else if (val.toString().charAt(0) == 'R') 
                {
                    listB.add(new Text(val.toString().substring(2)));
                }
            }//End_For
            Text C=new Text();
            float age=0;
            int count=0;
            float avgAge;
            //Actual Joining of two files
            if(!listA.isEmpty() && !listB.isEmpty())
            {
                for(Text A : listA)
                {
                    String frd[]=A.toString().split(",");
                    
                            for(int i=0;i<frd.length;i++)
                            {
                                if(myMap.containsKey(frd[i]))
                                {
                                    String[] ageCalu=myMap.get(frd[i]).split(":");
                                     Date now = new Date();
                                        int nowMonth = now.getMonth()+1;
                                        int nowYear = now.getYear()+1900;
                                        String[] cal=ageCalu[2].toString().split("/");
                                       int result = nowYear - Integer.parseInt(cal[2]);

                                        if (Integer.parseInt(cal[0]) > nowMonth) {
                                            result--;
                                        }
                                        else if (Integer.parseInt(cal[0]) == nowMonth) {
                                            int nowDay = now.getDate();

                                            if (Integer.parseInt(cal[1]) > nowDay) {
                                                result--;
                                            }
                                        }
                                        age+=result;
                                        count++;
                                }
                            }
                            avgAge=(float)(age/count);
                            String S="";
                            
                            for(Text B:listB)
                            {
                                S=B.toString()+","+new Text(new FloatWritable((float) avgAge).toString());
                            }
                            
                            C.set(S);
                    }
                
            }
            context.write(new Text(key.toString()),C);    
            }
        
    }
    public static class UserRatingsMapper extends Mapper<LongWritable, Text, UserPageWritable, Text> 
    {
        private Long outkey = new Long(0L);
        public Long getOutkey() {
            return outkey;
        }



        public void setOutkey(Long outkey) {
            this.outkey = outkey;
        }



        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
            
            String[] m=value.toString().split("\t");
            Long l=Long.parseLong(m[0]);
            outkey=l;
            if(m.length==2){
            String line[] = m[1].split(",");
            
             context.write(new UserPageWritable(Float.parseFloat(m[0]),Float.parseFloat(line[5])), new Text(m[1].toString()));
            }        
        }
                
            }
    
        
    
    public static class UserPageWritable implements  WritableComparable<UserPageWritable>  {

            private Float userId;
            private Float friendId;

            public Float getUserId() {
            return userId;
        }
        public void setUserId(Float userId) {
            this.userId = userId;
        }
        
        
        
        public Float getFriendId() {
            return friendId;
        }
        public void setFriendId(Float friendId) {
            this.friendId = friendId;
        }
        public UserPageWritable(Float user, Float friend1) {
                // TODO Auto-generated constructor stub
                this.userId=user;
                this.friendId=friend1;
            }
            public UserPageWritable(){}

            public void readFields(DataInput in) throws IOException {
              userId = in.readFloat();
              friendId = in.readFloat();
            }

            public void write(DataOutput out) throws IOException {
              out.writeFloat(userId);;
              out.writeFloat(friendId);;
            }
          
            public int compareTo(UserPageWritable o) {
                // TODO Auto-generated method stub
                
                 
                int result = userId.compareTo(o.userId);
                if (result != 0) {
                    return result;
                }
                return this.friendId.compareTo(o.friendId);
               /* if(0 == result) {
                    result = friendId.compareTo(o.friendId);
                }
                return result;*/
            }
            @Override
            public String toString() {
            return userId.toString() + ":" + friendId.toString();
            }
            @Override
            public boolean equals(Object obj) {
                if (obj == null) {
                    return false;
                }
                if (getClass() != obj.getClass()) {
                    return false;
                }
                final UserPageWritable other = (UserPageWritable) obj;
                if (this.userId != other.userId && (this.userId == null || !this.userId.equals(other.userId))) {
                    return false;
                }
                if (this.friendId != other.friendId && (this.friendId == null || !this.friendId.equals(other.friendId))) {
                    return false;
                }
                return true;
            }
            @Override
            public int hashCode() {
                return this.userId.hashCode() * 163 + this.friendId.hashCode();
            }
    }    
    
    
    
    public class TemperaturePartitioner extends Partitioner<UserPageWritable, Text>{
        @Override
        public int getPartition(UserPageWritable temperaturePair, Text nullWritable, int numPartitions) {
            return temperaturePair.getFriendId().hashCode() % numPartitions;
        }
    }
    
    
    public static class SecondarySortBasicCompKeySortComparator extends WritableComparator {

          public SecondarySortBasicCompKeySortComparator() {
                super(UserPageWritable.class, true);
            }

            @SuppressWarnings("rawtypes")
			@Override
            public int compare(WritableComparable w1, WritableComparable w2) {
                UserPageWritable key1 = (UserPageWritable) w1;
                UserPageWritable key2 = (UserPageWritable) w2;

                int cmpResult = -1*key1.getFriendId().compareTo(key2.getFriendId());
                
                
                return cmpResult;
            }
        }
    public static class SecondarySortBasicGroupingComparator extends WritableComparator {
          public SecondarySortBasicGroupingComparator() {
                super(UserPageWritable.class, true);
            }

            @SuppressWarnings("rawtypes")
			@Override
            public int compare(WritableComparable w1, WritableComparable w2) {
                UserPageWritable key1 = (UserPageWritable) w1;
                UserPageWritable key2 = (UserPageWritable) w2;
                return -1*key1.getFriendId().compareTo(key2.getFriendId());
            }
        }
    public static class UserRatingsMoviesReducer extends Reducer<UserPageWritable, Text, Text, Text> 
    {
        int i=0;
        TreeMap<String,String> hass=new TreeMap<String, String>();        
    
                 
        public void reduce(UserPageWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            // Clear our lists
            
            
            for(Text t:values)
            {
                //has.put(key.userId.toString(), t.toString());
                if(hass.size()<20)
                {
                    hass.put(key.userId.toString(), t.toString());
                     context.write(new Text(t.toString().split(",")[0]), new Text(t));
                }
            }
        }
    }

    
    
    

    //Driver code
    @SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception 
    {

        Path outputDirIntermediate1 = new Path(args[3] + "_int1");
        Path outputDirIntermediate2 = new Path(args[4] + "_int2");
        
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        conf.set("businessdata",otherArgs[0]);
        // get all args
        if (otherArgs.length != 5)
        {
            System.err.println("Usage: JoinExample <in> <in2> <in3> <out>");
            System.exit(2);
        }
        
        Job job = new Job (conf, "join1 ");
        job.setJarByClass(AverageAge.class);
        job.setReducerClass(UserRatingsReducer.class);

        MultipleInputs.addInputPath(job, new Path(otherArgs[1]),TextInputFormat.class, UserMapper.class );
        MultipleInputs.addInputPath(job, new Path(otherArgs[2]),TextInputFormat.class, RatingsMapper.class );

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
    
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job,outputDirIntermediate1);
        
        Job job1 = new Job(new Configuration(), "join2");
        job1.setJarByClass(AverageAge.class);
        
        // Set mapper and the average posts per user
        FileInputFormat.addInputPath(job1, new Path(args[3] + "_int1"));
        
        job1.setMapOutputKeyClass(UserPageWritable.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setPartitionerClass(TemperaturePartitioner.class);
         job1.setMapperClass(UserRatingsMapper.class);
    //        job1.setReducerClass(UserRatingsMoviesReducer.class);
        job1.setSortComparatorClass(SecondarySortBasicCompKeySortComparator.class);
        job1.setGroupingComparatorClass(SecondarySortBasicGroupingComparator.class);
        job1.setReducerClass(UserRatingsMoviesReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        //job1.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job1,outputDirIntermediate2);
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
            
    }
}
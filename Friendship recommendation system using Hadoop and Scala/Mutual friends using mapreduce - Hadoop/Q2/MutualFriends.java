package MutualFriends.mf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class MutualFriends extends Configured implements Tool{


	public static class FriendsMapper
	extends Mapper<LongWritable, Text, UserPageWritable, Text> {
		LongWritable userA =new LongWritable();
		LongWritable userB =new LongWritable();
		LongWritable user =new LongWritable();
		Long oneA=new Long(-1L);
		Long twoB=new Long(-1L);
		Long threeC=new Long(-1L);
		Long temp=new Long(-1L);
		String user1 ="";
		String user2 ="";
		public void setup(Context context) {
			Configuration config = context.getConfiguration();
			user1 = config.get("userA");
			user2 = config.get("userB");
			oneA=Long.parseLong(user1);
			twoB=Long.parseLong(user2);
		}
		int count=0;
		private Text m_id = new Text();
		private Text m_others = new Text();
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split("\t");

			String subject = split[0];
			threeC=Long.parseLong(subject);
			if(split.length==2)
			{
				String others = split[1];
				if((threeC.equals(oneA)) || (threeC.equals(twoB)))
				{
					m_others.set(others);
					if(threeC.equals(oneA))
						temp=twoB;
					else
						temp=oneA;
					if(threeC.compareTo(temp) < 0 )
					{
						context.write(new UserPageWritable(threeC,temp),m_others );

					}
					else
					{
						context.write(new UserPageWritable(temp,threeC),m_others );
					}             
				}

			}
		}
	}


	public static class FriendsReducer
	extends Reducer<UserPageWritable, Text, UserPageWritable, Text> {
		private Text m_result = new Text();
		HashMap<String, Integer> hash = new HashMap<String, Integer>();
		// Calculates intersection of two given Strings, i.e. friends lists
		private HashSet<Integer> intersection(String s1, String s2) {

			HashSet<Integer> h1 = new HashSet<Integer>();
			HashSet<Integer> h2 = new HashSet<Integer>();
			if(null!=s1){
				String[] s=s1.split(",");
				for(int i=0;i<s.length;i++)
				{
					h1.add(Integer.parseInt(s[i]));
				}
			}
			if(null!=s2)
			{
				String[] sa=s2.split(",");
				for(int i=0;i<sa.length;i++)
				{
					if(h1.contains(Integer.parseInt(sa[i])))
					{
						h2.add(Integer.parseInt(sa[i]));
					}
				}
			}

			return h2;


		}

		public void reduce(UserPageWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String[] combined = new String[2];
			int cur = 0;
			for(Text value : values) {
				combined[cur++] = value.toString();
			}
			//			context.write(key, new Text(combined[0]));
			//			context.write(key, new Text(combined[1]));
			combined[0] = combined[0].replaceAll("[^0-9,]", "");
			if(null!=combined[1])
				combined[1] = combined[1].replaceAll("[^0-9,]", "");
			//			context.write(key, new Text(combined[0]));
			//			context.write(key, new Text(combined[1]));
			HashSet<Integer> ca=intersection(combined[0], combined[1]);



			context.write(key, new Text(StringUtils.join(",", ca)));

		}
	}
	public static class UserPageWritable implements  WritableComparable<UserPageWritable>  {

		private Long userId;
		private Long friendId;

		public UserPageWritable(Long user, Long friend1) {
			// TODO Auto-generated constructor stub
			this.userId=user;
			this.friendId=friend1;
		}
		public UserPageWritable(){}

		public void readFields(DataInput in) throws IOException {
			userId = in.readLong();
			friendId = in.readLong();
		}

		public void write(DataOutput out) throws IOException {
			out.writeLong(userId);;
			out.writeLong(friendId);;
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
	public static void main(String args[]) throws Exception {
		// Standard Job setup procedure.
		int res = ToolRunner.run(new Configuration(), new MutualFriends(), args);
		System.exit(res);

	}
	public int run(String[] otherArgs) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		//String[] otherArgs = new GenericOptionsParser(conf, args0).getRemainingArgs();		// get all args
		if (otherArgs.length != 4) {
			System.err.println("Usage: UserRatedStanford <inbusiness> <inbusiness> <review> <out>");
			System.exit(2);
		}


		conf.set("userA", otherArgs[0]);
		conf.set("userB", otherArgs[1]);

		Job job = new Job(conf, "InlineArgument");
		job.setJarByClass(MutualFriends.class);


		job.setMapperClass(FriendsMapper.class);
		job.setReducerClass(FriendsReducer.class);

		job.setOutputKeyClass(UserPageWritable.class);

		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[2]));

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

		boolean sucess = job.waitForCompletion(true);
		return (sucess ? 0 : 1);
		//return 0;
	}
}
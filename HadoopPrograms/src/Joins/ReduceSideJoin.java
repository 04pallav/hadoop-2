// https://github.com/adamjshook/mapreducepatterns
package Joins;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReduceSideJoin {

	public static class LeftSideMapper extends Mapper<Object, Text, Text, Text> {
		private Text joinkey = new Text();
		private Text record = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] data = value.toString().split(",");
			String strKey = data[0];
			String strRecord = value.toString().replace(strKey + ",", "");
			strRecord = "left," + strRecord;
			joinkey.set(strKey);
			record.set(strRecord);
			context.write(joinkey, record);
		}
	}

	public static class RightSideMapper extends
			Mapper<Object, Text, Text, Text> {
		private Text joinkey = new Text();
		private Text record = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] data = value.toString().split(",");
			String strKey = data[0];
			String strRecord = value.toString().replace(strKey + ",", "");
			strRecord = "right," + strRecord;
			joinkey.set(strKey);
			record.set(strRecord);
			context.write(joinkey, record);
		}
	}

	public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
		private static ArrayList<String> leftSide = new ArrayList<String>();
		private static ArrayList<String> rightSide = new ArrayList<String>();
		private String typeOfJoin = null;

		@Override
		public void setup(Context context) {
			typeOfJoin = context.getConfiguration().get("joinType");
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// Clearing list - Not sure
			leftSide.clear();
			rightSide.clear();

			// Gathering lists
			for (Text text : values) {
				String row = text.toString();
				if (row.startsWith("left,")) {
					row = row.replace("left,", "");
					leftSide.add(row);
				} else if (row.startsWith("right,")) {
					row = row.replace("right", "");
					rightSide.add(row);
				}
			}

			if (typeOfJoin.equalsIgnoreCase("inner"))
				innerJoin(context, key);
			else if (typeOfJoin.equalsIgnoreCase("left"))
				leftJoin(context, key);
			else if (typeOfJoin.equalsIgnoreCase("right"))
				rightJoin(context, key);
		}

		private static void innerJoin(Context context, Text key)
				throws IOException, InterruptedException {
			if (!leftSide.isEmpty() && !rightSide.isEmpty()) {
				for (String leftside : leftSide)
					for (String rightside : rightSide)
						context.write(key, new Text(leftside.replace("\n", "")
								+ "," + rightside));
			}

		}

		private static void leftJoin(Context context, Text key)
				throws IOException, InterruptedException {
			for (String leftside : leftSide) {
				if (rightSide.isEmpty())
					context.write(key, new Text(leftside));
				else
					for (String rightside : rightSide)
						context.write(key, new Text(leftside.replace("\n", "")
								+ "," + rightside));
			}
		}

		private static void rightJoin(Context context, Text key)
				throws IOException, InterruptedException {
			for (String rightside : rightSide) {
				if (leftSide.isEmpty())
					context.write(key, new Text(rightside));
				else
					for (String leftside : leftSide)
						context.write(new Text("Key: " + key.toString()),
								new Text(leftside.replace("\n", "") + ","
										+ rightside));
			}
		}

	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String joinType;
		Path leftSideDir, rightSideDir, outputDir;

		// Checking
		String[] myArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (myArgs.length != 4) {
			System.err
					.println("Error: Expecting format \"<leftSideDir rightSideDir outPutDir (left|right|inner)>\"");
		}

		// Retrieving arguments
		leftSideDir = new Path(myArgs[0]);
		rightSideDir = new Path(myArgs[1]);
		outputDir = new Path(myArgs[2]);
		joinType = myArgs[3];

		String[] joinTypes = { "inner", "left", "right" };

		boolean joinFound = false;
		for (int count = 0; count < joinTypes.length; count++)
			if (joinType.equalsIgnoreCase(joinTypes[count]))
				joinFound = true;

		if (!joinFound)
			System.err
					.println("Join type non-existent. Choose from inner, left, and right");

		// Configuring Job
		Job job = new Job(conf, "ReduceSideJoin");
		job.getConfiguration().set("joinType", joinType);
		job.setJarByClass(Joins.ReduceSideJoin.class);

		// Adding input and output paths
		MultipleInputs.addInputPath(job, leftSideDir, TextInputFormat.class,
				LeftSideMapper.class);
		MultipleInputs.addInputPath(job, rightSideDir, TextInputFormat.class,
				RightSideMapper.class);

		job.setReducerClass(JoinReducer.class);

		FileOutputFormat.setOutputPath(job, outputDir);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 3);
		
	}

}

// https://github.com/adamjshook/mapreducepatterns
package Joins;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.util.GenericOptionsParser;

public class CompositeJoin {
	public static class CompositeJoinMapper extends MapReduceBase implements
			Mapper<Text, TupleWritable, Text, Text> {

		@Override
		public void map(Text key, TupleWritable value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// output.collect(new Text("Hello"), new Text("World"));
			output.collect((Text) value.get(0), (Text) value.get(1));
		}

	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		JobConf conf = new JobConf("CompositeJoin");
		conf.setJarByClass(Joins.CompositeJoin.class);

		String joinType;
		Path leftSideDir, rightSideDir, outputDir;
		int numOfReduceTask = 0;

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
		for (String join: joinTypes)
			if (joinType.equalsIgnoreCase(join))
				joinFound = true;

		if (!joinFound)
			System.err
					.println("Join type non-existent. Choose from inner, left, and right");
		String joinDesc = "inner";
		if (joinType.equalsIgnoreCase("left") || joinType.equals("right"))
			joinDesc = "outer";
		// Configuring Job
		conf.setMapperClass(CompositeJoinMapper.class);
		conf.setNumReduceTasks(numOfReduceTask);
		conf.setInputFormat(CompositeInputFormat.class);
		conf.set("mapred.join.expr", CompositeInputFormat.compose(
				joinDesc.toLowerCase(), TextInputFormat.class,
				leftSideDir, rightSideDir));

		TextOutputFormat.setOutputPath(conf, outputDir);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		// Run Job
		RunningJob job = JobClient.runJob(conf);
		while (!job.isComplete())
			Thread.sleep(100);
		System.exit(job.isSuccessful() ? 0 : 2);
	}

}

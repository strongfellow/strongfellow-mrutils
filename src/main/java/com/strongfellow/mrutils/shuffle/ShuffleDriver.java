package com.strongfellow.mrutils.shuffle;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleDriver {

	private static final Logger logger = LoggerFactory.getLogger(ShuffleDriver.class);
	
	private final List<String> inputs;
	private final String output;
	private final int numReducers;
	
	public ShuffleDriver(List<String> inputs, String output, int numReducers) {
		this.inputs = inputs;
		this.output = output;
		this.numReducers = numReducers;
	}

	public void main() throws IOException {
		logger.info("begin main");
		JobConf job = new JobConf(ShuffleDriver.class);
		job.setJobName(ShuffleDriver.class.getName());
		
		job.setInputFormat(SequenceFileInputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		
		job.setMapperClass(BlockShuffleMapper.class);
		job.setReducerClass(BlockShuffleReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);
		
		for (String input : this.inputs) {
			FileInputFormat.addInputPath(job, new Path(input));
		}
		FileOutputFormat.setOutputPath(job, new Path(this.output));
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setNumReduceTasks(this.numReducers);
		
		JobClient.runJob(job);
		logger.info("SUCCESS");

	}

}

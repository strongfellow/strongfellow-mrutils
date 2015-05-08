package com.strongfellow.mrutils.blockcrawl;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BlockCrawlDriver {

	private static final Logger logger = LoggerFactory.getLogger(BlockCrawlDriver.class);
	
	private final List<String> inputs;
	private final String output;
	
	public BlockCrawlDriver(List<String> inputs, String out) {
		this.inputs = inputs;
		this.output = out;
	}
	
	public void main() throws IOException {
		logger.info("begin main");
		JobConf job = new JobConf(BlockCrawlDriver.class);
		job.setJobName(BlockCrawlDriver.class.getName());
		
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		
		job.setMapperClass(BlockCrawlMapper.class);
		job.setReducerClass(BlockCrawlReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		for (String input : this.inputs) {
			FileInputFormat.addInputPath(job, new Path(input));
		}
		FileOutputFormat.setOutputPath(job, new Path(this.output));
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		
		JobClient.runJob(job);
		logger.info("SUCCESS");
	}
}

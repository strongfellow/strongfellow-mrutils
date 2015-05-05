package com.strongfellow.mrutils.blockcrawl;

import java.io.IOException;

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


public class BlockCrawlDriver {

	public static void main(String[] args) throws IOException {
		JobConf job = new JobConf(BlockCrawlDriver.class);
		job.setJobName(BlockCrawlDriver.class.getName());
		
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		
		job.setMapperClass(BlockCrawlMapper.class);
		job.setReducerClass(BlockCrawlReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path("in"));
		FileOutputFormat.setOutputPath(job, new Path("out"));
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		
		JobClient.runJob(job);
	}
}

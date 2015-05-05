package com.strongfellow.mrutils.blockcrawl;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockCrawlMapper implements Mapper<LongWritable, Text, IntWritable, Text> {

	private static final Random random = new Random();
	private static final Logger logger = LoggerFactory.getLogger(BlockCrawlMapper.class);
	private final IntWritable key = new IntWritable();
	
	@Override
	public void configure(JobConf arg0) {
		logger.info("configured");
	}

	@Override
	public void close() throws IOException {
		logger.info("closed");
	}

	@Override
	public void map(LongWritable ignored, Text line,
			OutputCollector<IntWritable, Text> collector, Reporter reporter)
			throws IOException {
		this.key.set(random.nextInt());
		collector.collect(key, line);
	}

}

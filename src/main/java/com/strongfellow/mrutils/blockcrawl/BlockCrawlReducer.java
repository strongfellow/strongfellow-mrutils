package com.strongfellow.mrutils.blockcrawl;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.strongfellow.utils.BlockParser;
import com.strongfellow.utils.ParseException;
import com.strongfellow.utils.data.Block;

public class BlockCrawlReducer implements Reducer<IntWritable, Text, NullWritable, BytesWritable> {

	private final BlockParser blockParser = new BlockParser();
	private final AmazonS3 s3 = new AmazonS3Client();
	private static final Logger logger = LoggerFactory.getLogger(BlockCrawlReducer.class);

	private final NullWritable key = NullWritable.get();
	private final BytesWritable value = new BytesWritable();
	@Override
	public void configure(JobConf conf) {
		logger.info("configured");
	}

	@Override
	public void close() throws IOException {
		logger.info("closed");
	}

	@Override
	public void reduce(IntWritable ignored, Iterator<Text> values,
			OutputCollector<NullWritable, BytesWritable> collector, Reporter reporter)
			throws IOException {
		while (values.hasNext()) {
			Text tsv = values.next();
			logger.info(tsv.toString());
			String[] tokens = tsv.toString().split("\\s+");
			String start = tokens[0];
			String end = tokens[1];
			while (!start.equals(end)) {
				String key = String.format("networks/f9beb4d9/blocks/%s/payload", start);
				logger.info("getting {}", key);
				S3Object object = s3.getObject("strongfellow.com", key); 
				byte[] bytes = IOUtils.toByteArray(object.getObjectContent());
				value.set(bytes, 0, bytes.length);
				collector.collect(this.key, this.value);
				try {
					Block block = blockParser.parse(bytes);
					start = block.getHeader().getPreviousBlock();
				} catch (ParseException e) {
					System.exit(1);
				}
			}
		}
	}

}

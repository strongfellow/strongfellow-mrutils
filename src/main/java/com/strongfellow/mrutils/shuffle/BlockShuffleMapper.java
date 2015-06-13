package com.strongfellow.mrutils.shuffle;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.strongfellow.utils.BlockParser;
import com.strongfellow.utils.ParseException;
import com.strongfellow.utils.data.Block;

public class BlockShuffleMapper implements Mapper<NullWritable, BytesWritable, Text, BytesWritable>{

	private BlockParser blockParser = new BlockParser();
	private static final Logger logger = LoggerFactory.getLogger(BlockShuffleMapper.class);
	
	@Override
	public void configure(JobConf conf) {
		logger.info("configured");
	}

	@Override
	public void close() throws IOException {
		logger.info("closed");
	}

	@Override
	public void map(NullWritable ignored, BytesWritable block,
			OutputCollector<Text, BytesWritable> output, Reporter reporter)
			throws IOException {
		try {
			Block blk = blockParser.parse(block.getBytes());
			output.collect(new Text(blk.getBlockHash()), block);
		} catch (ParseException e) {
			logger.error("bad parse", e);
			reporter.incrCounter("PARSER", "ERROR", 1);
		}
	}

}

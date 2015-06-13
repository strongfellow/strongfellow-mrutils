package com.strongfellow.mrutils.shuffle;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockShuffleReducer implements Reducer<Text, BytesWritable, NullWritable, BytesWritable> {

	private static final Logger logger = LoggerFactory.getLogger(BlockShuffleReducer.class);
	
	@Override
	public void configure(JobConf conf) {
		logger.info("configured");
	}

	@Override
	public void close() throws IOException {
		logger.info("closed");
	}
	@Override
	public void reduce(Text hash, Iterator<BytesWritable> blocks,
			OutputCollector<NullWritable, BytesWritable> output, Reporter reporter)
			throws IOException {
		int i = 0;
		while (blocks.hasNext()) {
			output.collect(NullWritable.get(), blocks.next());
			i++;
		}
		reporter.incrCounter("UNIQUE_HASHES", "COUNT-" + Integer.toString(i), i);

	}

}

package com.strongfellow.mrutils;

import java.io.IOException;
import java.util.List;

import javax.activation.CommandMap;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.strongfellow.mrutils.blockcrawl.BlockCrawlDriver;
import com.strongfellow.mrutils.shuffle.ShuffleDriver;

public class Main {

	@Parameters(separators = "=", commandDescription = "Crawl from s3")
	private static class CommandCrawl {
	 
	  @Parameter(description = "The list of input files")
	  private List<String> inputs;
	
	  @Parameter(names={"--output"}, description = "the output path")
	  private String output;
	  
	  @Parameter(names={"--numReduceTasks"}, description="how many reduce tasks")
	  private int numReducers = 23;
	}

	@Parameters(separators = "=", commandDescription = "Crawl from s3")
	private static class CommandShuffle {
	 
	  @Parameter(description = "The list of input files")
	  private List<String> inputs;
	
	  @Parameter(names={"--output"}, description = "the output path")
	  private String output;
	  
	  @Parameter(names={"--numReduceTasks"}, description="how many reduce tasks")
	  private int numReducers = 23;
	}

	public static void main(String[] args) throws IOException {
		JCommander jc = new JCommander();
		CommandCrawl cc = new CommandCrawl();
		CommandShuffle cs = new CommandShuffle();
		jc.addCommand("crawl", cc);
		jc.addCommand("shuffle", cs);
		jc.parse(args);
		
		if ("crawl".equals(jc.getParsedCommand())) {
			BlockCrawlDriver bcd = new BlockCrawlDriver(cc.inputs, cc.output, cc.numReducers);
			bcd.main();
		} else if ("shuffle".equals(jc.getParsedCommand())) {
			ShuffleDriver sd = new ShuffleDriver(cs.inputs, cs.output, cs.numReducers);
			sd.main();
		} else {
			throw new RuntimeException("no command passed in");
		}
	}
}

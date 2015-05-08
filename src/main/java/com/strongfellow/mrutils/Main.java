package com.strongfellow.mrutils;

import java.io.IOException;
import java.util.List;

import javax.activation.CommandMap;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.strongfellow.mrutils.blockcrawl.BlockCrawlDriver;

public class Main {

	@Parameters(separators = "=", commandDescription = "Crawl from s3")
	private static class CommandCrawl {
	 
	  @Parameter(description = "The list of input files")
	  private List<String> inputs;
	
	  @Parameter(names={"--output"}, description = "the output path")
	  private String output;
	}

	public static void main(String[] args) throws IOException {
		JCommander jc = new JCommander();
		CommandCrawl cc = new CommandCrawl();
		jc.addCommand("crawl", cc);
		jc.parse(args);
		
		if ("crawl".equals(jc.getParsedCommand())) {
			BlockCrawlDriver bcd = new BlockCrawlDriver(cc.inputs, cc.output);
			bcd.main();
		} else {
			throw new RuntimeException("no command passed in");
		}		
	}
}

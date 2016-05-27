package org.luismesalas.storm.topology;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.logging.Logger;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.luismesalas.storm.bolt.LangIdentifier;
import org.luismesalas.storm.bolt.StatsToFile;
import org.luismesalas.storm.bolt.TextTokenizer;
import org.luismesalas.storm.spout.FolderWatcher;

public class StormTopologyLocal {
    private static final int DEFAULT_PROB = 60;
    private static final String DEFAULT_OUTPUT = "/mm_disk/output";
    private static final String DEFAULT_INPUT = "/mm_disk/input";
    final static Logger logger = Logger.getLogger(StormTopologyLocal.class.getName());

    public static void main(String[] args) throws InterruptedException {

	ArgumentParser argParser = createArgParser();
	try {
	    Namespace parsedArguments = argParser.parseArgs(args);
	    Config conf = new Config();

	    String configFileParam = parsedArguments.getString("configuration");
	    String inputPathParam = parsedArguments.getString("input");
	    String outputPathParam = parsedArguments.getString("output");
	    Integer limitParam = parsedArguments.getInt("limit");

	    if (limitParam < 0 || limitParam > 100) {
		throw new ArgumentParserException("Invalid value for limit. Allowed value: a positive integer between 0 and 100.",
			argParser);
	    }

	    Properties topologyProperties = new Properties();
	    InputStream inputStreamConfig = new FileInputStream(configFileParam);
	    topologyProperties.load(inputStreamConfig);

	    for (Entry<Object, Object> obj : topologyProperties.entrySet()) {
		conf.put(obj.getKey().toString(), obj.getValue().toString());
	    }

	    TopologyBuilder builder = new TopologyBuilder();
	    builder.setSpout("folder-watcher", new FolderWatcher());
	    builder.setBolt("lang-identifier", new LangIdentifier()).shuffleGrouping("folder-watcher");
	    builder.setBolt("text-tokenizer", new TextTokenizer()).shuffleGrouping("lang-identifier");
	    builder.setBolt("stats-to-file", new StatsToFile()).shuffleGrouping("text-tokenizer");

	    conf.put("input", inputPathParam);
	    conf.put("output", outputPathParam);
	    conf.put("limit", new Double(limitParam / 100));
	    conf.setDebug(true);

	    conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, Integer.valueOf(topologyProperties.getProperty("spouts")));
	    conf.put(Config.TOPOLOGY_WORKERS, Integer.valueOf(topologyProperties.getProperty("workers")));
	    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, topologyProperties.getProperty("worker_childopts"));

	    try {
		LocalCluster cluster = new LocalCluster();
		String topologyName = "StormTopology-" + new Date().getTime();
		cluster.submitTopology(topologyName, conf, builder.createTopology());
	    } catch (Exception e) {
		logger.severe("Topology already alive!");
	    }
	} catch (ArgumentParserException | IOException e) {
	    logger.severe(e.getMessage());
	}

    }

    private static ArgumentParser createArgParser() {

	ArgumentParser argParser = ArgumentParsers.newArgumentParser("storm-example.jar").description(
		"This topology is an example that reads a folder and classify all the files inside this folder by language.");

	argParser.addArgument("-c", "--configuration").required(true).help("Storm topology and global configuration file.\n");
	argParser.addArgument("-i", "--input").required(false).setDefault(DEFAULT_INPUT)
		.help("Input folder to process. Default: " + DEFAULT_INPUT + "\n");
	argParser.addArgument("-o", "--ouput").required(false).setDefault(DEFAULT_OUTPUT)
		.help("Output folder to store the process result. Default: " + DEFAULT_OUTPUT + "\n");
	argParser.addArgument("-l", "--limit").type(Integer.class).required(false).setDefault(DEFAULT_PROB)
		.help("Language probability limit. Allowed value: a positive integer between 0 and 100. Default: " + DEFAULT_PROB + ".\n");

	return argParser;
    }
}

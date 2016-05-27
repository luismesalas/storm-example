package org.luismesalas.storm.bolt;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class TextTokenizer extends BaseRichBolt {

    private static final long serialVersionUID = 6224344161122150174L;

    final static Logger logger = Logger.getLogger(TextTokenizer.class.getName());
    OutputCollector _collector;
    Set<String> limitCharsToExclude;

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
	try {
	    _collector = collector;
	    limitCharsToExclude = loadCharsToExcludeFromFile(conf.get("nontoken_file").toString());
	} catch (IOException e) {
	    logger.severe("Error on inicialization on TextTokenizer bolt: " + e.getMessage());
	    e.printStackTrace();
	}
    }

    @Override
    public void execute(Tuple input) {

	String content = input.getString(0);
	String filepath = input.getString(1);
	Object langarrayObj = input.getValue(2);

	try {
	    Map<String, Integer> tokensFrequency = tokenizeText(content);

	    if (!tokensFrequency.isEmpty()) {
		logger.info("#Tokens identified for document " + filepath + ": " + tokensFrequency.size());
	    } else {
		logger.info("No tokens identified for document " + filepath);
	    }
	    _collector.emit(input, new Values(content, filepath, langarrayObj, tokensFrequency));
	    _collector.ack(input);

	} catch (Exception e) {
	    logger.severe("Exception in TextTokenizer bolt: " + e.getMessage());
	    _collector.fail(input);
	}

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	declarer.declare(new Fields("content", "filepath", "langarray", "tokensfreq"));
    }

    private Map<String, Integer> tokenizeText(String content) {
	Map<String, Integer> result = new HashMap<String, Integer>();
	StringTokenizer stringTokenizer = new StringTokenizer(content.toLowerCase());
	while (stringTokenizer.hasMoreElements()) {
	    String token = stringTokenizer.nextElement().toString();
	    token = cleanUpToken(token);
	    if (!"".equals(token)) {
		if (result.containsKey(token)) {
		    Integer newCount = result.get(token) + 1;
		    result.put(token, newCount);
		} else {
		    result.put(token, 1);
		}
	    }
	}
	return result;
    }

    private Set<String> loadCharsToExcludeFromFile(String path) throws IOException {
	String line;
	InputStreamReader streamReader = null;
	InputStream file = new FileInputStream(path);
	Set<String> nonTokens = new HashSet<String>();

	streamReader = new InputStreamReader(file, Charset.forName("UTF-8"));
	BufferedReader bufferedReader = new BufferedReader(streamReader);
	while ((line = bufferedReader.readLine()) != null && line != "") {
	    nonTokens.add(line.trim().toLowerCase());
	}
	bufferedReader.close();

	return nonTokens;
    }

    private String cleanUpToken(String token) {

	String result = token;

	while (endsWithExcludedChar(result)) {
	    if (result.length() > 1) {
		result = result.substring(0, result.length() - 1);
	    } else {
		result = "";
	    }
	}
	while (startsWithExcludedChar(result)) {
	    if (result.length() > 1) {
		result = result.substring(1, result.length());
	    } else {
		result = "";
	    }
	}

	return result;
    }

    private boolean endsWithExcludedChar(String text) {
	boolean result = false;
	for (String characterToExclude : limitCharsToExclude) {
	    if (text.endsWith(characterToExclude)) {
		result = true;
		break;
	    }
	}
	return result;
    }

    private boolean startsWithExcludedChar(String text) {
	boolean result = false;
	for (String characterToExclude : limitCharsToExclude) {
	    if (text.startsWith(characterToExclude)) {
		result = true;
		break;
	    }
	}
	return result;
    }

}

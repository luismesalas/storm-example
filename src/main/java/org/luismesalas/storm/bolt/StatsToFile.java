package org.luismesalas.storm.bolt;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.storm.shade.org.apache.commons.io.FileUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.luismesalas.storm.util.GeneralUtils;

import com.cybozu.labs.langdetect.Language;

public class StatsToFile extends BaseRichBolt {

    private static final int NUM_LANGUAGES_TOSHOW = 5;

    private static final long serialVersionUID = -5591869036995183101L;

    final static Logger logger = Logger.getLogger(StatsToFile.class.getName());
    OutputCollector _collector;
    String _inputPath;
    String _outputPath;
    Double _limit;

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
	try {
	    _collector = collector;
	    _inputPath = conf.get("input").toString();
	    _outputPath = conf.get("output").toString();
	    _limit = Double.parseDouble(conf.get("limit").toString());
	} catch (Exception e) {
	    logger.severe("Error on inicialization on StatsToFile bolt: " + e.getMessage());
	    e.printStackTrace();
	}
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(Tuple input) {

	@SuppressWarnings("unused")
	String content = input.getString(0);
	String filepath = input.getString(1);
	Object langarrayObj = input.getValue(2);
	Object tokensFrequencyObj = input.getValue(3);

	try {
	    ArrayList<Language> languages = (ArrayList<Language>) langarrayObj;
	    Map<String, Integer> tokensFrequency = (HashMap<String, Integer>) tokensFrequencyObj;

	    logger.info("Generating language stats for file: " + filepath);
	    String languageInfo = getLanguageSummary(languages);
	    logger.info("Generating tokens stats for file: " + filepath);
	    String fequencyInfo = getOrderedTokensFrequencySummary(tokensFrequency);

	    String langSubfolder = "ambiguous";
	    if (languages.get(0).prob >= _limit) {
		langSubfolder = languages.get(0).lang;
	    }

	    String outputSubFolderPath = _outputPath + File.separator + langSubfolder;

	    String fileOutPath = GeneralUtils.calculateDeltaPath(_inputPath, outputSubFolderPath, filepath) + "_stats.txt";
	    File fileOut = new File(fileOutPath);

	    if (!fileOut.getParentFile().exists()) {
		fileOut.getParentFile().mkdirs();
	    }

	    logger.info("Writing to file: " + fileOutPath);
	    FileUtils.writeStringToFile(fileOut, languageInfo + "\n\n" + fequencyInfo, Charset.forName("UTF-8"));

	    _collector.ack(input);

	} catch (Exception e) {
	    logger.severe("Exception in StatsToFile bolt: " + e.getMessage());
	    _collector.fail(input);
	}

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private String getOrderedTokensFrequencySummary(Map<String, Integer> tokensFrequency) {
	String result = "Tokens count stats\n\n";
	Object[] a = tokensFrequency.entrySet().toArray();

	Arrays.sort(a, new Comparator() {
	    public int compare(Object o1, Object o2) {
		int c = ((Map.Entry<String, Integer>) o2).getValue().compareTo(((Map.Entry<String, Integer>) o1).getValue());
		if (c == 0) {
		    c = ((Map.Entry<String, Integer>) o2).getKey().compareTo(((Map.Entry<String, Integer>) o1).getKey());
		}
		return c;
	    }
	});

	for (Object e : a) {
	    result += ((Map.Entry<String, Integer>) e).getKey() + ": " + ((Map.Entry<String, Integer>) e).getValue() + "\n";
	}
	return result;
    }

    private String getLanguageSummary(ArrayList<Language> languages) {
	String result = "Language probability stats\n\n";

	for (int i = 0; (i < languages.size() && i < NUM_LANGUAGES_TOSHOW); i++) {
	    result += languages.get(i).lang + ": " + languages.get(i).prob + "\n";
	}

	return result;
    }

}

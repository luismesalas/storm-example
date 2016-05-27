package org.luismesalas.storm.bolt;

import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.cybozu.labs.langdetect.Language;

public class LangIdentifier extends BaseRichBolt {

    private static final long serialVersionUID = 6224344161122150174L;

    final static Logger logger = Logger.getLogger(LangIdentifier.class.getName());
    OutputCollector _collector;
    Double limit;

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
	try {
	    _collector = collector;
	    DetectorFactory.loadProfile(conf.get("lang_profiles").toString());
	    DetectorFactory.setSeed(0);
	    limit = Double.parseDouble(conf.get("limit").toString());
	} catch (LangDetectException e) {
	    logger.severe("Error on inicialization on LangIdentifier bolt: " + e.getMessage());
	    e.printStackTrace();
	}
    }

    @Override
    public void execute(Tuple input) {

	String content = input.getString(0);
	String filepath = input.getString(1);

	try {

	    Detector detector = DetectorFactory.create();
	    detector.append(content);
	    detector.detect();
	    ArrayList<Language> languages = detector.getProbabilities();

	    if (languages != null) {
		if (languages.size() > 0 && languages.get(0).prob >= limit) {
		    logger.info("Language " + languages.get(0).lang + " identified for file " + filepath + " with a probability of "
			    + languages.get(0).prob);
		    _collector.emit(input, new Values(content, filepath, languages));
		    _collector.ack(input);
		} else {
		    logger.info("Could not identify language for file: " + filepath + ". Ambiguous file.");
		    _collector.emit(input, new Values(content, filepath, languages));
		    _collector.ack(input);

		}
	    } else {
		logger.warning("Could not perform language identification on file: " + filepath);
		_collector.fail(input);
	    }
	} catch (Exception e) {
	    logger.severe("Exception in LangIdentifier bolt: " + e.getMessage());
	    _collector.fail(input);
	}
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	declarer.declare(new Fields("content", "filepath", "langarray"));
    }

}

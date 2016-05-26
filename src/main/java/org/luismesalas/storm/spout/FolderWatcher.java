package org.luismesalas.storm.spout;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.logging.Logger;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.luismesalas.storm.util.GeneralUtils;

public class FolderWatcher extends BaseRichSpout {

    private static final long serialVersionUID = 4959474912622154093L;
    private static final Logger logger = Logger.getLogger(FolderWatcher.class.getName());
    SpoutOutputCollector _collector;
    WatchService _watcher;
    String _inputPath;
    String _outputPath;
    File _currentFile;

    Queue<File> _fqueue = new LinkedList<File>();

    @Override
    public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
	_collector = collector;
	try {
	    _watcher = FileSystems.getDefault().newWatchService();
	    _inputPath = conf.get("inputPath").toString();
	    _outputPath = conf.get("outputPath").toString();
	    _currentFile = null;

	    _outputPath = conf.get("outputPath").toString();
	    File outputFolder = new File(_outputPath);
	    if (!outputFolder.exists()) {
		outputFolder.mkdirs();
	    }

	    logger.info("Reading files from: " + _inputPath);
	    logger.info("Process output files are going to" + _outputPath);

	    Path dir = Paths.get(_inputPath);
	    dir.register(_watcher, ENTRY_CREATE);

	    enqueueFileList();

	} catch (IOException e) {
	    logger.severe(e.getMessage());
	}

    }

    @Override
    public void nextTuple() {

	WatchKey key;
	try {
	    key = _watcher.poll();

	    if (key != null) {

		int count = 0;

		for (WatchEvent<?> event : key.pollEvents()) {

		    WatchEvent.Kind<?> kind = event.kind();

		    // Registered only for ENTRY_CREATE events. Eventually an OVERFLOW event can occur regardless if events are lost or
		    // discarded
		    if (kind == OVERFLOW) {
			logger.warning("Input folder overflow");
			_fqueue.clear();
			enqueueFileList();
			continue;
		    }

		    // The file absoulte path is the context of the event.
		    @SuppressWarnings("unchecked")
		    WatchEvent<Path> ev = (WatchEvent<Path>) event;
		    Path filename = ev.context();

		    logger.info("File: " + filename);

		    if (filename != null && !filename.equals("")) {
			enqueueFileList();
		    }

		}

		if (count != 0) {
		    logger.info("#Enqueued elements: " + count);
		}

		// Reset the key. This is crucial if you want to receive further watch events. If the key is no longer valid, the directory
		// is inaccessible so exits the loop.
		key.reset();

	    }

	    File currentFile = _fqueue.poll();
	    if (currentFile != null) {

		logger.info("Reading file: " + currentFile.getAbsolutePath());
		String fileContent = GeneralUtils.readFile(currentFile.getAbsolutePath());

		File outputCandidate = new File(GeneralUtils.calculateDeltaPath(_inputPath, _outputPath, currentFile.getAbsolutePath()));

		if (fileContent == "" || outputCandidate.exists()) {
		    {
			logger.warning("Fail processing" + currentFile);

			File dest = new File(GeneralUtils.calculateDeltaPath(_inputPath, _outputPath + File.separator + "failed",
				currentFile.getAbsolutePath()));

			if (!dest.getParentFile().exists()) {
			    dest.getParentFile().mkdirs();
			}

			if (currentFile.renameTo(dest)) {
			    logger.info("Original file moved to: " + dest.getAbsolutePath());
			    GeneralUtils.deleteEmptyParents(currentFile.getParentFile(), _inputPath);
			} else {
			    logger.warning("Can't move file: " + currentFile.getAbsolutePath());
			}
		    }
		} else {
		    _currentFile = currentFile;
		    _collector.emit(new Values(fileContent, currentFile.getAbsolutePath()), currentFile.getAbsolutePath());
		}
	    } else {
		Utils.sleep(2000);
	    }

	} catch (Exception e) {
	    e.printStackTrace();
	    return;
	}

    }

    @Override
    public void ack(Object id) {

	logger.info("File " + id.toString() + " processed with success");
	try {
	    File file = new File(id.toString());

	    File dest = new File(GeneralUtils.calculateDeltaPath(_inputPath, _outputPath + File.separator + "processed",
		    file.getAbsolutePath()));

	    if (!dest.getParentFile().exists()) {
		dest.getParentFile().mkdirs();
	    }

	    if (file.renameTo(dest)) {
		logger.info("Original file moved to: " + dest.getAbsolutePath());
		GeneralUtils.deleteEmptyParents(file.getParentFile(), _inputPath);
	    } else {
		logger.warning("Can't move file: " + id);
	    }

	} catch (Exception e) {

	    logger.severe(e.getMessage());
	    return;
	}

    }

    @Override
    public void fail(Object id) {

	logger.warning("Failed tuple: " + id);

	try {

	    File file = new File(id.toString());
	    File dest = new File(GeneralUtils.calculateDeltaPath(_inputPath, _outputPath + File.separator + "failed",
		    file.getAbsolutePath()));

	    if (!dest.getParentFile().exists()) {
		dest.getParentFile().mkdirs();
	    }

	    if (file.renameTo(dest)) {
		logger.info("Original file moved to: " + dest.getAbsolutePath());
		GeneralUtils.deleteEmptyParents(file.getParentFile(), _inputPath);
	    } else {
		logger.warning("Can't move file: " + id);
	    }
	} catch (Exception e) {
	    logger.severe(e.getMessage());
	    return;
	}

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	declarer.declare(new Fields("content", "filepath"));
    }

    private void enqueueFileList() {

	File inputFolder = new File(_inputPath);
	List<File> filesToProcess = new ArrayList<File>();
	GeneralUtils.getFilesRecursively(inputFolder, filesToProcess);
	Collections.sort(filesToProcess);

	if (filesToProcess != null && filesToProcess.size() > 0) {
	    for (File file : filesToProcess) {
		_fqueue.add(file);
	    }
	    logger.info("#Files enqueued: " + filesToProcess.size());
	}

    }
}

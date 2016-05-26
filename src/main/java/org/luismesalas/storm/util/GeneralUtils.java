package org.luismesalas.storm.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public final class GeneralUtils {

    public static void getFilesRecursively(File inputDir, List<File> result) {
	getFilesRecursively(inputDir, null, null, result);
    }

    public static void getFilesRecursively(File inputDir, String extension, List<File> result) {
	getFilesRecursively(inputDir, extension, null, result);
    }

    public static void getFilesRecursively(File inputDir, String extension, String folderFilter, List<File> result) {
	File[] files = inputDir.listFiles();
	for (File file : files) {
	    if (file.isDirectory()) {
		getFilesRecursively(file, extension, folderFilter, result);
	    } else if (extension == null || (file.getName().toLowerCase().endsWith(extension))
		    && (folderFilter == null || file.getParentFile().getName().equals(folderFilter))) {
		result.add(file);
	    }
	}
    }

    public static void copyFile(File src, File dest) throws IOException {
	InputStream in = new FileInputStream(src);

	OutputStream out = new FileOutputStream(dest);

	byte[] buffer = new byte[1024];

	int length;
	while ((length = in.read(buffer)) > 0) {
	    out.write(buffer, 0, length);
	}

	in.close();
	out.close();

    }

    public static void deleteEmptyParents(File currentParentFile, String inputDir) {
	if (currentParentFile != null && !currentParentFile.getAbsolutePath().equals(inputDir)
		&& (currentParentFile.listFiles() == null || currentParentFile.listFiles().length == 0)) {
	    File nextParent = currentParentFile.getParentFile();
	    if (currentParentFile.delete()) {
		deleteEmptyParents(nextParent, inputDir);
	    }
	}
    }

    public static String calculateDeltaPath(String inputPath, String outputPath, String currentFilePath) {
	return outputPath + File.separator + currentFilePath.replace(inputPath, "");
    }

    public static String readFile(String path) throws IOException {
	return readFile(path, StandardCharsets.UTF_8);
    }

    public static String readFile(String path, Charset encoding) throws IOException {
	byte[] encoded = Files.readAllBytes(Paths.get(path));
	return new String(encoded, encoding);
    }

}
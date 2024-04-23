package vn.bnh.datadiff.services;

import java.util.Properties;

public interface FileProcessorService {
    Properties readPropertiesFile(String filePath);

    boolean FileChecker(String filePath);

}
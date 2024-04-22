package vn.bnh.datadiff.services;

import java.util.Properties;

public interface FileProcessorService {
    public Properties readPropertiesFile(String filePath);

    public boolean FileChecker(String filePath);
    
}
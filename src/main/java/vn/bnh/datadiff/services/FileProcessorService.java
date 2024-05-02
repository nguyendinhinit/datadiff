package vn.bnh.datadiff.services;

import java.io.File;
import java.util.Properties;

public interface FileProcessorService {
    public Properties readPropertiesFile(File file);
    public Properties readPropertiesFileV2(File file);

    public boolean FileChecker(String filePath);
    
}
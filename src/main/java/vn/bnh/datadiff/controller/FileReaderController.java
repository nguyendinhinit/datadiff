package vn.bnh.datadiff.controller;

import vn.bnh.datadiff.service.FileReaderService;

import java.util.Properties;

public class FileReaderController {
    FileReaderService fileReaderService = new FileReaderService();
    public Properties readFilePath(String filePath) {
        // Read file path
        return  fileReaderService.readPropertiesFile(filePath);
    }
}

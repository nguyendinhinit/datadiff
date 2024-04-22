package vn.bnh.datadiff.controllers;

import vn.bnh.datadiff.services.FileProcessorService;
import vn.bnh.datadiff.services.impl.FileProcessorServiceImpl;

import java.util.Properties;

public class FileProcessorController {
    FileProcessorService fileProcessorService = new FileProcessorServiceImpl();

    public Properties readPropertiesFile(String filePath) {
        return fileProcessorService.readPropertiesFile(filePath);
    }
}
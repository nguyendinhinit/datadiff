package vn.bnh.datadiff.controllers;

import vn.bnh.datadiff.services.FileProcessorService;
import vn.bnh.datadiff.services.impl.FileProcessorServiceImpl;

import java.io.File;
import java.util.Properties;

public class FileProcessorController {
    FileProcessorService fileProcessorService = new FileProcessorServiceImpl();

    public Properties readPropertiesFile(File file) {
        return fileProcessorService.readPropertiesFile(file);
    }
    public Properties readPropertiesFileV2(File file){
        return fileProcessorService.readPropertiesFileV2(file);
    }
}
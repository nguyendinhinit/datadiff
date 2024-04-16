package vn.bnh.datadiff.controller;

import vn.bnh.datadiff.service.FileChecker;
import vn.bnh.datadiff.service.Impl.FileCheckerImpl;

import java.io.File;

public class FileCheckerController {
    FileChecker fileChecker = new FileCheckerImpl();

    public void checkFileExist(File filePath) {
        fileChecker.checkFile(filePath);
    }
}

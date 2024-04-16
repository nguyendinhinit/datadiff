package vn.bnh.datadiff.service.Impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vn.bnh.datadiff.service.FileChecker;

import java.io.File;


/*

This class to check the file path and file name exist or not exist in the system before reading the file.
 */
public class FileCheckerImpl implements FileChecker {
    Logger log4j = LogManager.getLogger(FileCheckerImpl.class);

    @Override
    public boolean checkFile(File file) {
        // Check file exist or not
        String filePath = file.getAbsolutePath();
        log4j.info("Checking file exist: " + file.getAbsolutePath());
        try {
            if (file.exists()) {
                log4j.info("File exist at the path: " + filePath);
                return true;
            }
        } catch (Exception e) {
            log4j.error("Error: " + e);
        }
        log4j.info("File not exist at the path: " + filePath);
        return false;
    }
}

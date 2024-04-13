package vn.bnh.datadiff;

import vn.bnh.datadiff.controller.FileReaderController;

import java.util.Properties;
import java.util.logging.Logger;

public class Application {
    Logger logger = Logger.getLogger(Application.class.getName());
    FileReaderController filerReaderController = new FileReaderController();
    public void run(String filePath) {
        // Set file path
       Properties properties =  filerReaderController.readFilePath(filePath);
        // Print file path
        logger.info(properties.getProperty("mysql_connection_string"));
    }

}

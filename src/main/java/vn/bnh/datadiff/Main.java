package vn.bnh.datadiff;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vn.bnh.datadiff.mapping.DataTypeMapper;

import java.util.ArrayList;
import java.util.Map;


public class Main {
    static Logger log4j = LogManager.getLogger(Main.class);

    /**
     * This class is the main class of the application. The application will receive 2 arguments from the command line.
     *
     * @arg 0: The path to the properties file
     * @arg 1: The feature that the application will run. The application will run the feature based on the value of this argument.
     */
    public static void main(String[] args) {
        try {
            String fileName = args[0];
            String feature = args[1];
            Application application = new Application();
            switch (feature) {
                case "validate":
                    application.runValidateMetadata(fileName);
                    break;
                case "count job":
                    application.runCountJob(fileName);
                    break;
                case "missing table":
                    application.runMissingTable(fileName);
                    break;
                case "constraints and indexes":
                    application.runConstrainsAndIndexes(fileName);
            }
        } catch (Exception e) {
            log4j.error("Not enough parameter.");
        }
    }
}
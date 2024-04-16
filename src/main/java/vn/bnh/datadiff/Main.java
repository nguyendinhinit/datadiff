package vn.bnh.datadiff;

import org.jetbrains.annotations.NotNull;

import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.logging.Logger;

public class Main {
    static Logger logger = Logger.getLogger(Main.class.getName());
    public static void main(String @NotNull [] args) throws SQLException, FileNotFoundException {
        // Get file path from command line argument
        String filePath = args[0];
        // Create Application object and run it
        Application application = new Application();
        application.run(filePath);



    }
}

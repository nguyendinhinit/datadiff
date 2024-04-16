package vn.bnh.datadiff;

import org.jetbrains.annotations.NotNull;
public class Main {

    public static void main(String @NotNull [] args) {
        // Get file path from command line argument
        String filePath = args[0];
        // Create Application object and run it
        Application application = new Application();
        application.run(filePath);

    }
}

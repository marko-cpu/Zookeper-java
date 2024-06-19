package rs.raf.pds.faulttolerance;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;



public class Snapshot {

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    private final Date date = new Date();
    private final String dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);

    public void startSnapshot(AccountService accountService, ReplicatedLog replicatedLog, long interval, TimeUnit timeUnit, String fileName) {
        Runnable task = () -> {
            accountService.takeSnapshot();
            replicatedLog.takeSnapshot();
            System.out.println("Snapshot taken successfully at " + dateFormat);
            clearLogFile(fileName);
        };

        scheduler.scheduleAtFixedRate(task, 0, interval, timeUnit);
    }

    private void clearLogFile(String fileName) {
        try (FileWriter fw = new FileWriter(new File(fileName))) {
            fw.write("");
            System.out.println("Log file cleared successfully " + fileName);
        } catch (IOException e) {
            System.err.println("Error clearing log file: " + e.getMessage());
        }
    }
}

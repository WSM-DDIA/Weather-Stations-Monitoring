package bitCask;

import bitCask.command.CommandFactory;
import bitCask.storage.BitCask;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final int port = 4240;
    private static BitCask bitCask;

    public static void main(String[] args) throws FileNotFoundException {
        bitCask = new BitCask(args[0]);

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                System.out.println("initiating merge and compaction");
                bitCask.mergeAndCompaction();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, 5, 30, TimeUnit.SECONDS);

        MultiServer multiServer = new MultiServer(bitCask);
        try {
            multiServer.start(port);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
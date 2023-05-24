package bitCask.storage;

import java.util.concurrent.atomic.AtomicInteger;

public class FileConcurrency {
    AtomicInteger readCount = new AtomicInteger(0);

    String FileName;

    public FileConcurrency(String FileName) {
        this.FileName = FileName;
    }
}

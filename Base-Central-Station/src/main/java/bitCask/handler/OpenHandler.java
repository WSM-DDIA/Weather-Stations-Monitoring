package bitCask.handler;

import bitCask.storage.BitCask;
import com.google.common.primitives.Ints;

import java.io.IOException;

public record OpenHandler(String directory) implements MessageHandler {
    @Override
    public byte[] execute(BitCask bitCask) {
        try {
            bitCask.open(directory);
            System.out.println("Directory found");
            return Ints.toByteArray(200);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

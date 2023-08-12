package bitCask.handler;

import bitCask.storage.BitCask;
import com.google.common.primitives.Ints;

import java.io.IOException;

public record OpenHandler(String directory) implements MessageHandler {
    /**
     * Executes the open command which opens the directory
     *
     * @param bitCask {@link BitCask} instance
     * @return byte array representation of status
     */
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

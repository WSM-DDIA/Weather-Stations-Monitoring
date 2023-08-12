package bitCask.handler;

import bitCask.exception.DirectoryNotFoundException;
import bitCask.storage.BitCask;

import java.io.IOException;

public record GetHandler(byte[] key) implements MessageHandler {
    /**
     * Get the value of the key from the {@link BitCask} instance
     *
     * @param bitCask {@link BitCask} instance
     * @return byte array representation of the value
     */
    @Override
    public byte[] execute(BitCask bitCask) {
        try {
            return bitCask.get(key);
        } catch (IOException | DirectoryNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}

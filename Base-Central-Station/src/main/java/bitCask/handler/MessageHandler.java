package bitCask.handler;

import bitCask.storage.BitCask;

public interface MessageHandler {
    /**
     * Executes the command.
     *
     * @param bitCask {@link BitCask} instance
     * @return byte array representation of output
     */
    byte[] execute(BitCask bitCask);
}

package bitCask.command;

import bitCask.storage.BitCask;

public interface Command {
    /**
     * do the command which is implemented by the user
     *
     * @param keyValueStore the key value store
     * @return the result of the command
     */
    String execute(BitCask keyValueStore);
}

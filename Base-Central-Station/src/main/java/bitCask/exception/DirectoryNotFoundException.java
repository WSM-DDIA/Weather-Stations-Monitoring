package bitCask.exception;

public class DirectoryNotFoundException extends Exception {
    /**
     * Constructs a new directory not found exception with the specified detail message.
     *
     * @param message the detail message.
     */
    public DirectoryNotFoundException(String message) {
        super(message);
    }
}

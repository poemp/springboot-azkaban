package org.poem.exception;

/**
 * @author Yorke
 */
public class AzkabanException extends RuntimeException {

    private int code = 400;

    public AzkabanException(String message) {
        super(message);
    }

    public AzkabanException(int code, String message) {
        super(message);
        this.code = code;
    }
}

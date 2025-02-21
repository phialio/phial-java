package io.phial;

public class DuplicatedKeyException extends RuntimeException {
    public DuplicatedKeyException(String message) {
        super(message);
    }
}

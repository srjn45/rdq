package code.srjn.retry.async.registry;

public class FailedToExtractLambdaNameException extends RuntimeException {

    public FailedToExtractLambdaNameException() {
        super();
    }

    public FailedToExtractLambdaNameException(String msg) {
        super(msg);
    }

    public FailedToExtractLambdaNameException(Exception ex) {
        super(ex);
    }

}

package com.github.yafeiwang1240.sparkoperator.thrift;

public class ExplainResult {

    private boolean res;
    private String message;

    private ExplainResult(boolean res, String message) {
        this.res = res;
        this.message = message;
    }

    public static ExplainResult newFailResult(String message) {
        return new ExplainResult(false, message);
    }

    public static ExplainResult newSuccessResult() {
        return new ExplainResult(true, null);
    }

    public boolean isRes() {
        return res;
    }

    public void setRes(boolean res) {
        this.res = res;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "ExplainResult{" +
                "res=" + res +
                ", message='" + message + '\'' +
                '}';
    }
}

package com.github.yafeiwang1240;

import com.github.yafeiwang1240.sparkoperator.action.Broadcast;

public class Job {
    public static void main(String[] args) {
        Function function = new Broadcast();
        function.function();
    }
}

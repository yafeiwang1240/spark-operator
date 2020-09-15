package com.github.yafeiwang1240;

import com.github.yafeiwang1240.sparkoperator.output.PrintSink;

public class Job {
    public static void main(String[] args) {
        Function function = new PrintSink();
        function.function();
    }
}

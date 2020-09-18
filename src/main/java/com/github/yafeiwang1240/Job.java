package com.github.yafeiwang1240;

import com.github.yafeiwang1240.sparkoperator.output.HiveSink;

public class Job {
    public static void main(String[] args) {
        Function function = new HiveSink();
        function.function();
    }
}

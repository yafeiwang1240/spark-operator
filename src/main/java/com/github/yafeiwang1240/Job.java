package com.github.yafeiwang1240;

import com.github.yafeiwang1240.sparkoperator.output.RedisSink;

public class Job {
    public static void main(String[] args) {
        Function function = new RedisSink();
        function.function();
    }
}

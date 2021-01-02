package com.github.yafeiwang1240;

import com.github.yafeiwang1240.sparkoperator.transformation.keyvalue.RePartition;

public class Job {
    public static void main(String[] args) {
        Function function = new RePartition();

        function.function();
    }
}

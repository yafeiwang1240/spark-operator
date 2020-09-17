package com.github.yafeiwang1240;

import com.github.yafeiwang1240.sparkoperator.transformation.value.MapPartitions;

public class Job {
    public static void main(String[] args) {
        Function function = new MapPartitions();
        function.function();
    }
}

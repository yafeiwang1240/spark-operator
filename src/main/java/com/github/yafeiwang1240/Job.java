package com.github.yafeiwang1240;

import com.github.yafeiwang1240.sparkoperator.transformation.keyvalue.PartitionBy;

public class Job {
    public static void main(String[] args) {
        Function function = new PartitionBy();
        function.function();
    }
}

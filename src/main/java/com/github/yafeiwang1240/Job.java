package com.github.yafeiwang1240;

import com.github.yafeiwang1240.sparkoperator.transformation.keyvalue.Repartition;

public class Job {
    public static void main(String[] args) {
        Function function = new Repartition();

        function.function();
    }
}

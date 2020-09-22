package com.github.yafeiwang1240;

import com.github.yafeiwang1240.sparkoperator.output.hive.DataFrame;

public class Job {
    public static void main(String[] args) {
        Function function = new DataFrame();
        function.function();
    }
}

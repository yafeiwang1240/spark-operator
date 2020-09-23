package com.github.yafeiwang1240;

import com.github.yafeiwang1240.sparkoperator.problem.SortByKey;

public class Job {
    public static void main(String[] args) {
        Function function = new SortByKey();
        function.function();
    }
}

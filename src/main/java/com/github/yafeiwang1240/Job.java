package com.github.yafeiwang1240;

import com.github.yafeiwang1240.sparkoperator.action.SortByKey;

public class Job {
    public static void main(String[] args) {
        Function function = new SortByKey();
        function.function();
    }
}

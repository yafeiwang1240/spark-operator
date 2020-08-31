package com.github.yafeiwang1240;

import com.github.yafeiwang1240.sparkoperator.action.Aggregate;

public class Job {
    public static void main(String[] args) {
        Function function = new Aggregate();
        function.function();
    }
}

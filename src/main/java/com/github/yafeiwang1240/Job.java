package com.github.yafeiwang1240;

import com.github.yafeiwang1240.sparkoperator.problem.SystemExit;

public class Job {
    public static void main(String[] args) {
        Function function = new SystemExit();

        function.function();
    }
}

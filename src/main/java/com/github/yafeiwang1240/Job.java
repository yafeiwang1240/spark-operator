package com.github.yafeiwang1240;

import com.github.yafeiwang1240.sparkoperator.output.ESSink;

public class Job {
    public static void main(String[] args) {
        Function function = new ESSink();
        function.function();
    }
}

package com.github.yafeiwang1240;

import com.github.yafeiwang1240.sparkoperator.input.HBaseInput;

public class Job {
    public static void main(String[] args) {
        Function function = new HBaseInput();
        function.function();
    }
}

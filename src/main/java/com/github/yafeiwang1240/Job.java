package com.github.yafeiwang1240;

import com.github.yafeiwang1240.sparkoperator.action.Foreach;

public class Job {
    public static void main(String[] args) {
        Function function = new Foreach();
        function.function();
    }
}

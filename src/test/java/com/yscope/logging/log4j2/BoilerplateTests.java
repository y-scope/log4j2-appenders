package com.yscope.logging.log4j2;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BoilerplateTests {
    @Test
    void testGreeting() {
        assertEquals(Boilerplate.getGreeting(), "Hello, world!");
    }
}

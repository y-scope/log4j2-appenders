package com.yscope.logging.log4j2;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class BoilerplateTests {
    @Test
    public void testGreeting() {
        assertEquals("Hello, world!", Boilerplate.getGreeting());
    }
}

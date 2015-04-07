package com.innometrics.integration.app.recommender.ml.model;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author andrew, Innometrics
 */
public class UniqueQueueTest {
    UniqueQueue<String> toTest = new UniqueQueue<>();

    @Test
    public void testAddWithOrder() throws Exception {
        toTest.add("First");
        toTest.add("Second");
        toTest.add("Third");

        Assert.assertEquals("First", toTest.peek());
        Assert.assertEquals("First", toTest.poll());
        Assert.assertFalse(toTest.contains("First"));
    }

    @Test
    public void testAddWithReOrder() throws Exception {
        toTest.add("First");
        toTest.add("Second");
        toTest.add("Third");
        toTest.add("First");

        Assert.assertEquals("Second", toTest.peek());
        Assert.assertEquals("Second", toTest.poll());
        Assert.assertFalse(toTest.contains("Second"));
    }

    @Test
    public void testDuplicate() throws Exception {
        toTest.add("First");
        toTest.add("First");
        toTest.add("Second");
        toTest.add("First");
        Assert.assertTrue(toTest.size() == 2);
        Assert.assertEquals("Second", toTest.poll());
        Assert.assertFalse(toTest.contains("Second"));
    }
}
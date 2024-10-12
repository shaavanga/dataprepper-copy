/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.common.thread;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.kafka.common.KafkaMdc;
import org.slf4j.MDC;

import java.util.UUID;
import java.util.concurrent.ThreadFactory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaPluginThreadFactoryTest {

    @Mock
    private ThreadFactory delegateThreadFactory;
    @Mock
    private Thread innerThread;
    @Mock
    private Runnable runnable;
    private String pluginType;
    private String topic;

    @BeforeEach
    void setUp() {
        pluginType = UUID.randomUUID().toString();
        topic = UUID.randomUUID().toString();

        when(delegateThreadFactory.newThread(any(Runnable.class))).thenReturn(innerThread);
    }


    private KafkaPluginThreadFactory createObjectUnderTest() {
        return new KafkaPluginThreadFactory(delegateThreadFactory, pluginType);
    }

    private KafkaPluginThreadFactory createObjectUnderTestWithTopic() {
        return new KafkaPluginThreadFactory(delegateThreadFactory, pluginType, topic);
    }

    @Test
    void newThread_creates_thread_from_delegate() {
        assertThat(createObjectUnderTest().newThread(runnable), equalTo(innerThread));
    }

    @Test
    void newThread_with_topic_creates_thread_from_delegate() {
        assertThat(createObjectUnderTestWithTopic().newThread(runnable), equalTo(innerThread));
    }

    @Test
    void newThread_creates_thread_with_name() {
        final KafkaPluginThreadFactory objectUnderTest = createObjectUnderTest();


        final Thread thread1 = objectUnderTest.newThread(runnable);
        assertThat(thread1, notNullValue());
        verify(thread1).setName(String.format("kafka-%s-1", pluginType));

        final Thread thread2 = objectUnderTest.newThread(runnable);
        assertThat(thread2, notNullValue());
        verify(thread2).setName(String.format("kafka-%s-2", pluginType));
    }

    @ParameterizedTest
    @CsvSource({
            "abcd12,abcd12",
            "aBCd12,abcd12",
            "abcd-12,abcd-12",
            "has space,has-space",
            "has!character,has-character",
            "this-is-somewhat-too-long,this-is-somewhat-too"
    })
    void newThread_with_topic_creates_thread_with_name(
            final String topicName,
            final String expectedPrefix) {
        this.topic = topicName;
        final KafkaPluginThreadFactory objectUnderTest = createObjectUnderTestWithTopic();

        final Thread thread1 = objectUnderTest.newThread(runnable);
        assertThat(thread1, notNullValue());
        verify(thread1).setName(String.format("%s-kafka-%s-1", expectedPrefix, pluginType));

        final Thread thread2 = objectUnderTest.newThread(runnable);
        assertThat(thread2, notNullValue());
        verify(thread2).setName(String.format("%s-kafka-%s-2", expectedPrefix, pluginType));
    }

    @Test
    void newThread_creates_thread_with_wrapping_runnable() {
        createObjectUnderTest().newThread(runnable);

        final ArgumentCaptor<Runnable> actualRunnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(delegateThreadFactory).newThread(actualRunnableCaptor.capture());

        final Runnable actualRunnable = actualRunnableCaptor.getValue();

        assertThat(actualRunnable, not(equalTo(runnable)));

        verifyNoInteractions(runnable);
        actualRunnable.run();
        verify(runnable).run();
    }

    @Test
    void newThread_with_topic_creates_thread_with_wrapping_runnable() {
        createObjectUnderTestWithTopic().newThread(runnable);

        final ArgumentCaptor<Runnable> actualRunnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(delegateThreadFactory).newThread(actualRunnableCaptor.capture());

        final Runnable actualRunnable = actualRunnableCaptor.getValue();

        assertThat(actualRunnable, not(equalTo(runnable)));

        verifyNoInteractions(runnable);
        actualRunnable.run();
        verify(runnable).run();
    }

    @Test
    void newThread_creates_thread_that_calls_MDC_on_run() {
        createObjectUnderTest().newThread(runnable);

        final ArgumentCaptor<Runnable> actualRunnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(delegateThreadFactory).newThread(actualRunnableCaptor.capture());

        final Runnable actualRunnable = actualRunnableCaptor.getValue();

        final String[] actualKafkaPluginType = new String[1];
        doAnswer(a -> {
            actualKafkaPluginType[0] = MDC.get(KafkaMdc.MDC_KAFKA_PLUGIN_KEY);
            return null;
        }).when(runnable).run();

        actualRunnable.run();

        assertThat(actualKafkaPluginType[0], equalTo(pluginType));
    }

    @Test
    void newThread_with_topic_creates_thread_that_calls_MDC_on_run() {
        createObjectUnderTestWithTopic().newThread(runnable);

        final ArgumentCaptor<Runnable> actualRunnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(delegateThreadFactory).newThread(actualRunnableCaptor.capture());

        final Runnable actualRunnable = actualRunnableCaptor.getValue();

        final String[] actualKafkaPluginType = new String[1];
        doAnswer(a -> {
            actualKafkaPluginType[0] = MDC.get(KafkaMdc.MDC_KAFKA_PLUGIN_KEY);
            return null;
        }).when(runnable).run();

        actualRunnable.run();

        assertThat(actualKafkaPluginType[0], equalTo(pluginType));
    }
}
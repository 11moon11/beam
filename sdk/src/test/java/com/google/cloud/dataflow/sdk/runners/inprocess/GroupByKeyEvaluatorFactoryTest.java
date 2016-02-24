/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.inprocess;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.InProcessEvaluationContext;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.util.InProcessBundle;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.util.KeyedWorkItem;
import com.google.cloud.dataflow.sdk.util.KeyedWorkItems;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link GroupByKeyEvaluatorFactory}.
 */
@RunWith(JUnit4.class)
public class GroupByKeyEvaluatorFactoryTest {
  @Test
  public void testInMemoryEvaluator() throws Exception {
    TestPipeline p = TestPipeline.create();
    KV<String, Integer> firstFoo = KV.of("foo", -1);
    KV<String, Integer> secondFoo = KV.of("foo", 1);
    KV<String, Integer> thirdFoo = KV.of("foo", 3);
    KV<String, Integer> firstBar = KV.of("bar", 22);
    KV<String, Integer> secondBar = KV.of("bar", 12);
    KV<String, Integer> firstBaz = KV.of("baz", Integer.MAX_VALUE);
    PCollection<KV<String, Integer>> values =
        p.apply(Create.of(firstFoo, firstBar, secondFoo, firstBaz, secondBar, thirdFoo));
    PCollection<KV<String, WindowedValue<Integer>>> kvs =
        values.apply(new GroupByKey.ReifyTimestampsAndWindows<String, Integer>());
    PCollection<KeyedWorkItem<String, Integer>> groupedKvs =
        kvs.apply(new GroupByKeyEvaluatorFactory.InProcessGroupByKeyOnly<String, Integer>());

    CommittedBundle<KV<String, WindowedValue<Integer>>> inputBundle =
        InProcessBundle.unkeyed(kvs).commit(Instant.now());
    InProcessEvaluationContext evaluationContext = mock(InProcessEvaluationContext.class);

    UncommittedBundle<KeyedWorkItem<String, Integer>> fooBundle =
        InProcessBundle.keyed(groupedKvs, "foo");
    UncommittedBundle<KeyedWorkItem<String, Integer>> barBundle =
        InProcessBundle.keyed(groupedKvs, "bar");
    UncommittedBundle<KeyedWorkItem<String, Integer>> bazBundle =
        InProcessBundle.keyed(groupedKvs, "baz");

    when(evaluationContext.createKeyedBundle(inputBundle, "foo", groupedKvs)).thenReturn(fooBundle);
    when(evaluationContext.createKeyedBundle(inputBundle, "bar", groupedKvs)).thenReturn(barBundle);
    when(evaluationContext.createKeyedBundle(inputBundle, "baz", groupedKvs)).thenReturn(bazBundle);

    // The input to a GroupByKey is assumed to be a KvCoder
    @SuppressWarnings("unchecked")
    Coder<String> keyCoder =
        ((KvCoder<String, WindowedValue<Integer>>) kvs.getCoder()).getKeyCoder();
    TransformEvaluator<KV<String, WindowedValue<Integer>>> evaluator =
        new GroupByKeyEvaluatorFactory()
            .forApplication(
                groupedKvs.getProducingTransformInternal(), inputBundle, evaluationContext);

    evaluator.processElement(WindowedValue.valueInEmptyWindows(gwValue(firstFoo)));
    evaluator.processElement(WindowedValue.valueInEmptyWindows(gwValue(secondFoo)));
    evaluator.processElement(WindowedValue.valueInEmptyWindows(gwValue(thirdFoo)));
    evaluator.processElement(WindowedValue.valueInEmptyWindows(gwValue(firstBar)));
    evaluator.processElement(WindowedValue.valueInEmptyWindows(gwValue(secondBar)));
    evaluator.processElement(WindowedValue.valueInEmptyWindows(gwValue(firstBaz)));

    evaluator.finishBundle();

    assertThat(
        fooBundle.commit(Instant.now()).getElements(),
        contains(
            new KeyedWorkItemMatcher<String, Integer>(
                KeyedWorkItems.elementsWorkItem(
                    "foo",
                    ImmutableSet.of(
                        WindowedValue.valueInGlobalWindow(-1),
                        WindowedValue.valueInGlobalWindow(1),
                        WindowedValue.valueInGlobalWindow(3))),
                keyCoder)));
    assertThat(
        barBundle.commit(Instant.now()).getElements(),
        contains(
            new KeyedWorkItemMatcher<String, Integer>(
                KeyedWorkItems.elementsWorkItem(
                    "bar",
                    ImmutableSet.of(
                        WindowedValue.valueInGlobalWindow(12),
                        WindowedValue.valueInGlobalWindow(22))),
                keyCoder)));
    assertThat(
        bazBundle.commit(Instant.now()).getElements(),
        contains(
            new KeyedWorkItemMatcher<String, Integer>(
                KeyedWorkItems.elementsWorkItem(
                    "baz",
                    ImmutableSet.of(WindowedValue.valueInGlobalWindow(Integer.MAX_VALUE))),
                keyCoder)));
  }

  private <K, V> KV<K, WindowedValue<V>> gwValue(KV<K, V> kv) {
    return KV.of(kv.getKey(), WindowedValue.valueInGlobalWindow(kv.getValue()));
  }

  private static class KeyedWorkItemMatcher<K, V>
      extends BaseMatcher<WindowedValue<KeyedWorkItem<K, V>>> {
    private final KeyedWorkItem<K, V> myWorkItem;
    private final Coder<K> keyCoder;

    public KeyedWorkItemMatcher(KeyedWorkItem<K, V> myWorkItem, Coder<K> keyCoder) {
      this.myWorkItem = myWorkItem;
      this.keyCoder = keyCoder;
    }

    @Override
    public boolean matches(Object item) {
      if (item == null || !(item instanceof WindowedValue)) {
        return false;
      }
      WindowedValue<KeyedWorkItem<K, V>> that = (WindowedValue<KeyedWorkItem<K, V>>) item;
      Multiset<WindowedValue<V>> myValues = HashMultiset.create();
      Multiset<WindowedValue<V>> thatValues = HashMultiset.create();
      for (WindowedValue<V> value : myWorkItem.elementsIterable()) {
        myValues.add(value);
      }
      for (WindowedValue<V> value : that.getValue().elementsIterable()) {
        thatValues.add(value);
      }
      try {
        return myValues.equals(thatValues)
            && keyCoder
                .structuralValue(myWorkItem.key())
                .equals(keyCoder.structuralValue(that.getValue().key()));
      } catch (Exception e) {
        return false;
      }
    }

    @Override
    public void describeTo(Description description) {
      description
          .appendText("KeyedWorkItem<K, V> containing key ")
          .appendValue(myWorkItem.key())
          .appendText(" and values ")
          .appendValueList("[", ", ", "]", myWorkItem.elementsIterable());
    }
  }
}

package com.kvstore.core.storage.LSMTree.utils;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import it.unimi.dsi.fastutil.objects.ObjectIntMutablePair;


public class IteratorMerger<T extends Comparable<T>> implements Iterator<T> {

    Iterator<T>[] iterators;
    ObjectHeapPriorityQueue<Pair<T, Integer>> queue;

    @SafeVarargs
    public IteratorMerger(Iterator<T>... iterators) {
        this.iterators = iterators;
        queue = new ObjectHeapPriorityQueue<>(
                Comparator.comparing((Pair<T, Integer> a) -> a.first())
                        .thenComparingInt(Pair::second)
        );

        for (int i = 0; i < iterators.length; i++) {
            if (iterators[i].hasNext()) {
                queue.enqueue(new ObjectIntMutablePair<>(iterators[i].next(), i));
            }
        }
    }

    @Override
    public boolean hasNext() {
        return !queue.isEmpty();
    }

    @Override
    public T next() {
        if (queue.isEmpty()) {
            return null;
        }

        Pair<T, Integer> top = queue.dequeue();

        T result = top.first();
        int index = top.second();

        if (index == -1) {
            return result;
        }

        T next = iterators[index].next();
        int newIndex = iterators[index].hasNext() ? index : -1;
        queue.enqueue(top.first(next).second(newIndex));

        return result;
    }
}

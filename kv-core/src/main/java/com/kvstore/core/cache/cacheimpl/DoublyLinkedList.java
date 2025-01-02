package com.kvstore.core.cache.cacheimpl;

import lombok.Data;
import lombok.Getter;

@Getter
public class DoublyLinkedList<K, V> {
    private Node<K, V> head;
    private Node<K, V> tail;
    private int size;

    public DoublyLinkedList() {
        head = new Node<>(null, null);
        tail = new Node<>(null, null);
        head.next = tail;
        tail.prev = head;
        size = 0;
    }

    public void addFirst(Node<K, V> node) {
        node.next = head.next;
        node.prev = head;
        head.next.prev = node;
        head.next = node;
        size++;
    }

    public Node<K, V> removeLast() {
        if(this.isEmpty()) {
            return null;
        }
        Node<K, V> node = tail.prev;
        removeNode(node);
        return node;
    }

    public void moveToFront(Node<K, V> node) {
        removeNode(node);
        addFirst(node);
    }

    private void removeNode(Node<K, V> node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
        size--;
    }

    public boolean isEmpty() {
        return size == 0;
    }
}

package io.phial.memory;

public class SimpleHeap {
    private long[] heap;
    private int size;

    public SimpleHeap() {
        this.heap = new long[4096];
    }

    public void offer(long element) {
        if (this.size == this.heap.length) {
            var newHeap = new long[this.size * 2];
            System.arraycopy(this.heap, 0, newHeap, 0, this.size);
            this.heap = newHeap;
        }
        int i = this.size;
        while (i > 0) {
            int j = (i - 1) / 2;
            if (element < this.heap[j]) {
                this.heap[i] = this.heap[j];
                i = j;
            } else {
                break;
            }
        }
        this.heap[i] = element;
        ++this.size;
    }

    public long peek() {
        return this.size == 0 ? 0 : this.heap[0];
    }

    public long poll() {
        if (this.size == 0) {
            return 0;
        }
        long result = this.heap[0];
        long e = this.heap[this.size - 1];
        int i = 0;
        for (; ; ) {
            int j = i * 2 + 1;
            int k = j + 1;
            if (j >= this.size) {
                break;
            }
            long l = this.heap[j];
            if (k >= this.size) {
                if (e > l) {
                    this.heap[i] = l;
                    i = j;
                }
                break;
            }
            long r = this.heap[k];
            if (l < r) {
                if (e <= l) {
                    break;
                }
                this.heap[i] = l;
                i = j;
            } else {
                if (e <= r) {
                    break;
                }
                this.heap[i] = r;
                i = k;
            }
        }
        this.heap[i] = e;
        --this.size;
        return result;
    }
}

#pragma once

#include <iostream>
#include <list>

typedef void(*callback) (void*, void*);
typedef void(*setData) (void*, void*);

template <typename Data>
class RingBuffer {
    private:
        int m_from;
        int m_next;
        int m_count;
        Data* m_data;
    public:
        int m_size;

        RingBuffer(int size):m_from(0),m_next(0),m_count(0) {
            m_size = size;
            m_data = new Data[size];
        }
        ~RingBuffer() {
            delete []m_data;
            m_data = NULL;
        }

        int push(void* value, setData setFn) {
            if (m_count == m_size) {
                return -1;
            }
            
            setFn(&m_data[m_next], value);
            m_next = (m_next + 1) % m_size;
            m_count++;
            return (m_next > 0) ? m_next - 1 : m_size - 1;
        }

        bool isFull() {
            return m_count == m_size;
        }

        bool isEmpty() {
            return m_count == 0;
        }

        int getCount() {
            return m_count;
        }

        void reset(int from, int to) {
            m_from = (to + 1) % m_size;
            if (to >= from) {
                m_count -= (to - from + 1);
            } else {
                m_count -= (to + m_size - from + 1);
            }
        }

        void onData(void* obj, callback callFn, int from, int to) {
            if (m_count > 0) {
                if (from <= to) {
                    for (int i = from; i <= to; i++) {
                        (*callFn)(obj, &m_data[i]);
                    }
                } else {
                    for (int i = from; i < m_size; i++) {
                        (*callFn)(obj, &m_data[i]);
                    }
                    for (int i = 0; i < to; i++) {
                        (*callFn)(obj, &m_data[i]);
                    }
                    if (from != to) {
                        (*callFn)(obj, &m_data[to]);
                    }
                }
            }
        }
};

template <typename Data>
class Bucket {
    private:
        long m_ts;
        RingBuffer<Data> *m_ring;
        int m_from;
        int m_to;
    public:
        Bucket(long ts, RingBuffer<Data> *ring, int from, int to):m_ts(ts),m_ring(ring),m_from(from),m_to(to) {}
        ~Bucket() {
            m_ring = NULL;
        }

        long getTs() {
            return m_ts;
        }

        RingBuffer<Data> *getRingBuffer() {
            return m_ring;
        }

        void setTo(int to) {
            m_to = to;
        }

        int getFrom() {
            return m_from;
        }

        int getTo() {
            return m_to;
        }

        int size() {
            if (m_from <= m_to) {
                return m_to - m_from + 1;
            }
            return m_to + m_ring->m_size - m_from + 1;
        }
};

template <typename Data>
class RingBuffers {
    private:
        int m_size;
        std::list<Bucket<Data>*> m_buckets;
        RingBuffer<Data> *m_big_ring;

        Bucket<Data> *addBucket(RingBuffer<Data> *ringBuffer, long ts, void* value, setData setFn) {
            int index = ringBuffer->push(value, setFn);
            Bucket<Data> *bucket = new Bucket<Data>(ts, ringBuffer, index, index);
            m_buckets.push_back(bucket);
            return bucket;
        }
    public:
        RingBuffers(int size):m_size(size) {
            m_buckets = {};
            m_big_ring = new RingBuffer<Data>(size);
        }
        ~RingBuffers() {
            m_buckets.clear();
        }
        
        void add(long ts, void* value, setData setFn) {
            if (m_buckets.empty()) {
                addBucket(m_big_ring, ts, value, setFn);
            } else {
                Bucket<Data> *bucket = m_buckets.back();
                RingBuffer<Data> *ringBuffer = bucket->getRingBuffer();
                if (ringBuffer->isFull()) {
                    // RingBuffer is Full, Add Bucket for new RingBuffer()
                    RingBuffer<Data> *newBuffer;
                    if (m_big_ring->isEmpty()) {
                        newBuffer = m_big_ring;
                    } else {
                        newBuffer = new RingBuffer<Data>(m_size / 2);
                    }
                    addBucket(newBuffer, ts, value, setFn);
                } else {
                    int index = ringBuffer->push(value, setFn);
                    if (bucket->getTs() == ts) {
                        bucket->setTo(index);
                    } else {
                        m_buckets.push_back(new Bucket<Data>(ts, ringBuffer, index, index));
                    }
                }
            }
        }

        void expire(long ts) {
            for (auto it = m_buckets.begin(); it != m_buckets.end();) {
                Bucket<Data> *bucket = *it;
                if (bucket->getTs() <= ts) {
                    m_buckets.erase(it++);
                    bucket->getRingBuffer()->reset(bucket->getFrom(), bucket->getTo());

                    if (bucket->getRingBuffer()->isEmpty()) {
                        if (bucket->getRingBuffer() != m_big_ring) {
                            delete bucket->getRingBuffer();
                        }
                    }
                    delete bucket;
                } else {
                    it++;
                }
            }
        }

        void collect(long from, long to, void* obj, callback callFn) {
            for (auto it = m_buckets.begin(); it != m_buckets.end();it++) {
                Bucket<Data> *bucket = *it;
                if (bucket->getTs() >= from && bucket->getTs() <= to) {
                    bucket->getRingBuffer()->onData(obj, callFn, bucket->getFrom(), bucket->getTo());
                }
            }
        }

        int size() {
            int size = 0;
            for (auto it = m_buckets.begin(); it != m_buckets.end();it++) {
                Bucket<Data> *bucket = *it;
                size += bucket->size();
            }
            return size;
        }

        long getFrom() {
            if (m_buckets.empty()) {
                return 0;
            }
            return m_buckets.front()->getTs();
        }

        long getTo() {
            if (m_buckets.empty()) {
                return 0;
            }
            return m_buckets.back()->getTs();
        }
};
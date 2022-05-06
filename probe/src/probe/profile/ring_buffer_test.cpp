#include <stdio.h>
#include <iostream>

#include "ring_buffer.h"

using namespace std;

class Int {
    private:
        long m_ts;
        int m_value;
    public:
        Int():m_value(0) {}
        Int(long ts, int value):m_ts(ts), m_value(value) {}
        int getValue() {
            return m_value;
        }
        long getTs() {
            return m_ts;
        }
};

class Print {
    public:
        Print() {}
        void print(void *intValue) {
            Int intData = * (Int*)intValue;
            fprintf(stdout, "Data: %d at %ld\n", intData.getValue(), intData.getTs());
        }
};

static void printData(void* object, void* value) {
    Print* pObject = (Print*) object;
    pObject->print(value);
}


int main(int argc, char** argv) {
    RingBuffers<Int> *rings = new RingBuffers<Int>(20);

    /**
     * Ring-0 0  [ 0,  9] 10 [10, 19]
     * Ring-1 20 [20, 29] 
     * Ring-2 30 [30, 39]
     */
    for (int i = 0; i < 4; i++) {
        for (int j = 0; j < 10; j++) {
            Int *value = new Int(i * 10l, i * 10 + j);
            rings->add(i * 10l, *value);
        }
    }
    Print *print = new Print();
    rings->collect(0, 50, print, printData);

    fprintf(stdout, "Current Size: %d\n", rings->size());

    rings->expire(21);
    fprintf(stdout, "After Exipre Size: %d\n", rings->size());

    rings->collect(20, 40, print, printData);
    return 0;
}
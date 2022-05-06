#ifndef KINDLING_PROBE_PROFILE_PERF_H
#define KINDLING_PROBE_PROFILE_PERF_H

#include <perf/event.h>
#include <stdbool.h>

struct sample_type_data {
    struct {
        __u32    pid;
        __u32    tid;
    }    tid_entry;
    struct {
        __u32    cpu;
        __u32    reserved;
    }    cpu_entry;
    __u64 counter;
    struct {
        __u64   nr;
        __u64   ips[0];
    } callchain;
};

struct perfData {
    bool running;
    int sampleMs;
    int collectMs;

    void (*sample)(void* object, __u64 timestamp, struct sample_type_data *sample_data);
    void (*collect)(void* object, __u64 timestamp);
};

int perf(void* object, struct perfData *data);
#endif
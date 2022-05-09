#ifndef KINDLING_PROBE_PROFILE_PERF_H
#define KINDLING_PROBE_PROFILE_PERF_H

#include <perf/event.h>
#include <stdbool.h>

struct sample_type_data {
    struct {
        __u32    pid;
        __u32    tid;
    } tid_entry;
    __u64   time;
    struct {
        __u64   nr;
        __u64   ips[0];
    } callchain;
};

struct perfData {
    bool running;
    int sampleMs;
    int collectMs;

    void (*sample)(__u64 timestamp, struct sample_type_data *sample_data);
    void (*collect)(__u64 timestamp);
};

int perf(struct perfData *data);
#endif
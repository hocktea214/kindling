#include <cmath>
#include "src/probe/profile/profiler.h"
#include "src/probe/profile/flame_graph.h"
#include <iostream>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

using namespace std;

void do_profile(void* object, __u64 timestamp, struct sample_type_data *sample_data) {
    if (sample_data->tid_entry.pid == 0) {
        return;
    }
    FlameGraph *flame_graph = (FlameGraph*)object;
    flame_graph->RecordSampleData(timestamp, sample_data);
}

void do_collect(void* object, __u64 timestamp) {
    char str[50];
    time_t now = time(NULL);
    strftime(str, 50, "%x %X", localtime(&now));
    cout << "===== " << str << " =====" << endl;
    
    FlameGraph *flame_graph = (FlameGraph*)object;
    flame_graph->CollectData(timestamp);
}

Profiler::Profiler(int cache_keep_time) {
    perf_data = (struct perfData *)malloc(sizeof(struct perfData) * 1);
    perf_data->running = 0;
    perf_data->sampleMs = 10;
    perf_data->sample = do_profile;
    perf_data->collectMs = 1000;
    perf_data->collect = do_collect;
    flame_graph = new FlameGraph(cache_keep_time);
}

Profiler::~Profiler() {
    delete perf_data;
}

void Profiler::Start() {
    perf(flame_graph, perf_data);
}

void Profiler::Stop() {
    perf_data->running = 0;
}

string Profiler::GetOnCpuData(__u32 tid, vector<pair<__u64, __u64>> periods) {
    return ((FlameGraph*) flame_graph)->GetOnCpuData(tid, periods);
}
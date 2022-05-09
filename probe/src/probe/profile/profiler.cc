#include <cmath>
#include "src/probe/profile/profiler.h"
#include "src/probe/profile/flame_graph.h"
#include <iostream>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <ctime>

using namespace std;

struct ProfileCtx {
    FlameGraph *flame_graph;
} profile_ctx;

void do_profile(__u64 timestamp, struct sample_type_data *sample_data) {
    if (sample_data->tid_entry.pid == 0) {
        return;
    }
    fprintf(stdout, "Collect Time0: %lld, Time2: %lld\n", timestamp, sample_data->time);

    profile_ctx.flame_graph->RecordSampleData(timestamp, sample_data);
}

void do_collect(__u64 timestamp) {
    char str[50];
    time_t now = time(NULL);
    strftime(str, 50, "%x %X", localtime(&now));
    cout << "===== " << str << " =====" << endl;
    
    profile_ctx.flame_graph->CollectData(timestamp);
}

Profiler::Profiler(int cache_keep_time) {
    profile_ctx.flame_graph = new FlameGraph(cache_keep_time);

    perf_data_ = (struct perfData *)malloc(sizeof(struct perfData) * 1);
    perf_data_->running = 0;
    perf_data_->sampleMs = 10;
    perf_data_->sample = do_profile;
    perf_data_->collectMs = 1000;
    perf_data_->collect = do_collect;
}

Profiler::~Profiler() {
    delete perf_data_;
}

void Profiler::Start() {
    perf(perf_data_);
}

void Profiler::Stop() {
    perf_data_->running = 0;
}

void Profiler::EnableFlameFile(bool file) {
    profile_ctx.flame_graph->EnableFlameFile(file);
}

string Profiler::GetOnCpuData(__u32 tid, vector<pair<uint64_t, uint64_t>> &periods) {
    return profile_ctx.flame_graph->GetOnCpuData(tid, periods);
}
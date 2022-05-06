#include "src/probe/profile/flame_graph.h"
#include <cstddef>
#include <sys/types.h>
#include <time.h>
#include <string.h>

const char *kseperatorStack = ";";
const char *kseperatorBlank = " ";
const char *kseperatorAt = "@";
const char *kseperatorNextLine = "\n";
const char *kseperatorNextData = "|";

SampleData::SampleData() {}

SampleData::SampleData(struct sample_type_data *sample_data) {
    pid_ = sample_data->tid_entry.pid;
    tid_ = sample_data->tid_entry.tid;
    nr_ = sample_data->callchain.nr;
    memcpy(&ips_[0], sample_data->callchain.ips, nr_ * sizeof(sample_data->callchain.ips[0]));
}

string SampleData::addThreadInfo(string call_stack, bool host) {
    if (host) {
        call_stack.insert(0, kseperatorStack);
        call_stack.insert(0, std::to_string(tid_));
        call_stack.insert(0, kseperatorStack);
        call_stack.insert(0, std::to_string(pid_));
    }
    return call_stack;
}

string SampleData::GetString(BPFSymbolTable *symbol_table, int max_depth, bool host) {
    string call_stack = "";
    bool kernel = false, user = false, seperator = false;
    int depth = 0, pid = 0;
    __u64 ip = 0;
    for (__u64 i = 0; i < nr_; i++) {
        ip = ips_[i];
        if (ip == PERF_CONTEXT_KERNEL) {
            kernel = true;
            user = false; 
            pid = -1;
            continue;
        } else if (ip == PERF_CONTEXT_USER) {
            kernel = false;
            user = true;
            pid = pid_;
            continue;
        }
        if (kernel || user) {
            if (seperator) {
                call_stack.insert(0, kseperatorStack);
            } else {
                seperator = true;
            }
            call_stack.insert(0, symbol_table->GetSymbol(ip, pid));
            if (++depth >= max_depth) {
                return addThreadInfo(call_stack, host);
            }
        }
    }

    return addThreadInfo(call_stack, host);
}

AggregateData::AggregateData(__u32 tid, int max_depth, const char* kSeperatorCount, const char* kSeperatorNext, BPFSymbolTable *symbol_table) {
    tid_ = tid;
    max_depth_ = max_depth;
    kseperatorCount_ = kSeperatorCount;
    kseperatorNext_ = kSeperatorNext;
    symbol_table_ = symbol_table;
}

void AggregateData::Aggregate(void* data) {
    SampleData *sample_data = (SampleData*)data;
    if (sample_data->pid_ == 0 || (tid_ > 0 && sample_data->tid_ != tid_)) {
        return;
    }

    string call_stack = sample_data->GetString(symbol_table_, max_depth_, tid_ == 0);
    auto itr = agg_count_map_.find(call_stack);
    if (itr == agg_count_map_.end()) {
        agg_count_map_.insert(std::make_pair(call_stack, 1));
    } else {
        itr->second = itr->second + 1;
    }
}

string AggregateData::ToString() {
    string result = "";
    
    bool seperator = false;
    map<string, int>::iterator itr;
    for (itr = agg_count_map_.begin(); itr != agg_count_map_.end(); itr++) {
        if (seperator) {
            result.append(kseperatorNext_);
        } else {
            seperator = true;
        }
        result.append(itr->first);
        result.append(kseperatorCount_);
        result.append(std::to_string(itr->second));
    }
    return result;
}

static void aggTidData(void* object, void* value) {
    AggregateData* pObject = (AggregateData*) object;
    pObject->Aggregate(value);
}

FlameGraph::FlameGraph(int cache_keep_time) {
    cache_keep_time_ = cache_keep_time;
    sample_datas_ = new RingBuffers<SampleData>(1000);
    symbol_table_ = new BPFSymbolTable();
    resetLogFile();
}

FlameGraph::~FlameGraph() {
    delete &sample_datas_;
    delete symbol_table_;
}

void FlameGraph::RecordSampleData(__u64 timestamp, struct sample_type_data *sample_data) {
    SampleData *data = new SampleData(sample_data);
    sample_datas_->add(timestamp, *data);
}

void FlameGraph::CollectData(__u64 timestamp) {
    if (cache_keep_time_ == 0) {
        AggregateData *aggregateData = new AggregateData(0, 20, kseperatorBlank, kseperatorNextLine, symbol_table_); // Set 20.
        sample_datas_->collect(last_collect_time_, timestamp, aggregateData, aggTidData);
        fprintf(collect_file_, "%s\n", aggregateData->ToString().c_str());  // Write To File.
        resetLogFile();
    }

    fprintf(stdout, "Before Exipre Size: %d\n", sample_datas_->size());
    // Expire RingBuffer Datas.
    sample_datas_->expire(timestamp - cache_keep_time_);
    fprintf(stdout, "After Exipre Size: %d\n", sample_datas_->size());
    last_collect_time_ = timestamp;
}

string FlameGraph::GetOnCpuData(__u32 tid, vector<std::pair<__u64, __u64>> periods) {
    AggregateData *aggregateData = new AggregateData(tid, 1, kseperatorAt, kseperatorNextData, symbol_table_); // Set 1.

    __u64 start_time = 0, end_time = 0;
    int size = periods.size();
    for (int i = 0; i < size; i++) {
        start_time = periods[i].first / 1000000; // ns->ms
        end_time = (periods[i].first + periods[i].second) / 1000000; // ns->ms
        if (start_time < end_time) {
            fprintf(stdout, ">> Collect: %lld -> %lld, Duration: %lld\n", start_time, end_time, end_time-start_time);

            sample_datas_->collect(start_time, end_time, aggregateData, aggTidData);
        }
    }
    return aggregateData->ToString();
}

void FlameGraph::resetLogFile() {
    char collectFileName[128];
    time_t nowtime = time(NULL);
    tm *now = localtime(&nowtime);
    snprintf(collectFileName, sizeof(collectFileName), "flamegraph_%d_%d_%d.txt", now->tm_hour, now->tm_min, now->tm_sec);
    collect_file_ = fopen(collectFileName, "w+");
}

#ifndef KINDLING_PROBE_PROFILE_FLAMEGRAPH_H
#define KINDLING_PROBE_PROFILE_FLAMEGRAPH_H
extern "C" {
#include "src/probe/profile/perf/perf.h"
}
#include "src/probe/profile/bcc/symbol.h"
#include "src/probe/profile/ring_buffer.h"
#include <string>
#include <list>
#include <map>
#include <vector>
#include <bits/types/FILE.h>

using std::list;
using std::map;
using std::vector;
using std::string;
using std::string;

class SampleData {
  public:
    SampleData();
    SampleData(struct sample_type_data *sample_data);
    string GetString(BPFSymbolTable *symbol_table, int max_depth, bool host);

    __u32 pid_;
    __u32 tid_;
    __u64 nr_;
    __u64 ips_[256];
  private:
    string addThreadInfo(string call_stack, bool host);
};

class AggregateData {
  public:
    AggregateData(__u32 tid, int max_depth, const char* seperator_count, const char* seperator_next, BPFSymbolTable *symbolTable);
    void Aggregate(void* data);
    string ToString();

  private:
    __u32 tid_;
    int max_depth_;
    const char* kseperatorCount_;
    const char* kseperatorNext_;
    BPFSymbolTable *symbol_table_;
    map<string, int> agg_count_map_;
};

class FlameGraph {
 public:
  FlameGraph(int cache_keep_time);
  ~FlameGraph();
  void RecordSampleData(__u64 timestamp, struct sample_type_data *sample_data);
  void CollectData(__u64 timestamp);
  string GetOnCpuData(__u32 tid, vector<std::pair<__u64, __u64>> periods);

 private:
  void resetLogFile();

  int cache_keep_time_;
  __u64 last_collect_time_;
  RingBuffers<SampleData> *sample_datas_;
  BPFSymbolTable *symbol_table_;
  FILE *collect_file_;
};
#endif //KINDLING_PROBE_PROFILE_FLAMEGRAPH_H
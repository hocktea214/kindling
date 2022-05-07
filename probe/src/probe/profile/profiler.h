#ifndef KINDLING_PROBE_PROFILE_PROFILER_H
#define KINDLING_PROBE_PROFILE_PROFILER_H
extern "C" {
#include "src/probe/profile/perf/perf.h"
}
#include <string>
#include <vector>

class Profiler {
public:
    Profiler(int cache_keep_time);
    ~Profiler();
    void Start();
    void Stop();
    void EnableFlameFile(bool file);
    std::string GetOnCpuData(__u32 tid, std::vector<std::pair<uint64_t, uint64_t>> &periods);

private:
    struct perfData *perf_data_;
};

#endif //KINDLING_PROBE_PROFILE_PROFILER_

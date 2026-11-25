#ifndef DOWNLOAD_MANAGER_H
#define DOWNLOAD_MANAGER_H

#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <chrono>
#include "file_manager.h"


// PHASE 6

struct ChunkTask {
    uint32_t file_id = 0;
    uint32_t chunk_index = 0;
    uint32_t target_peer_id = 0;
    std::string target_ip;
    uint16_t target_port = 0;
    std::chrono::steady_clock::time_point sent_at;
    int attempts = 0;
};

struct ActiveDownload {
    uint32_t file_id = 0;
    std::string file_name;
    uint64_t file_size = 0;
    uint32_t chunk_size = DEFAULT_CHUNK_SIZE;
    uint32_t total_chunks = 0;
    std::vector<uint32_t> expected_checksums;
    std::unordered_set<uint32_t> completed_chunks;
    bool completed = false;
};

class DownloadManager {
public:
    explicit DownloadManager(
        uint32_t self_id,
        const std::string& data_dir,
        int chunk_timeout_ms = 800,
        int max_attempts = 5
    );

    // Starts tracking a download for a known FileMeta
    bool start_download(const FileMeta& file_meta, FileManager& file_mgr);

    // Selects next missing chunks to request across active peers that hold each chunk,
    // spreading requests across multiple source peers in parallel
    std::vector<ChunkTask> schedule_next_requests(
        uint32_t file_id,
        const FileManager& file_mgr,
        const std::unordered_map<uint32_t, std::pair<std::string, uint16_t>>& active_peer_endpoints,
        size_t max_new_requests = 8
    );

    // Re-queues timed-out in-flight chunk requests (and drops tasks for inactive/dead peers)
    void handle_timeouts_and_peer_loss(const std::unordered_set<uint32_t>& active_peer_ids);

    // Validates checksum, stores chunk via FileManager, and assembles file if all chunks arrive
    bool handle_chunk_received(
        uint32_t file_id,
        uint32_t chunk_index,
        uint32_t from_peer_id,
        const std::vector<uint8_t>& chunk_bytes,
        FileManager& file_mgr,
        bool& out_file_completed
    );

    bool is_downloading(uint32_t file_id) const;
    bool is_completed(uint32_t file_id) const;

private:
    uint32_t self_id_;
    std::string data_dir_;
    int chunk_timeout_ms_;
    int max_attempts_;
    size_t rr_source_cursor_ = 0;

    std::unordered_map<uint32_t, ActiveDownload> downloads_;
    // Key: FileManager::make_chunk_key(file_id, chunk_index)
    std::unordered_map<uint64_t, ChunkTask> in_flight_;

    bool assemble_file(const ActiveDownload& dl, const FileManager& file_mgr);
};

#endif // DOWNLOAD_MANAGER_H
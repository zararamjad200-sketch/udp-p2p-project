#include "../include/download_manager.h"
#include <filesystem>
#include <fstream>
#include <algorithm>

namespace fs = std::filesystem;

DownloadManager::DownloadManager(
    uint32_t self_id,
    const std::string& data_dir,
    int chunk_timeout_ms,
    int max_attempts
)
    : self_id_(self_id),
      data_dir_(data_dir),
      chunk_timeout_ms_(chunk_timeout_ms),
      max_attempts_(max_attempts) {}

bool DownloadManager::start_download(const FileMeta& file_meta, FileManager& file_mgr) {
    if (file_meta.file_id == 0 || file_meta.total_chunks == 0) {
        return false;
    }

    auto existing_it = downloads_.find(file_meta.file_id);
    bool was_already_completed = (existing_it != downloads_.end() && existing_it->second.completed);

    ActiveDownload dl;
    dl.file_id = file_meta.file_id;
    dl.file_name = file_meta.file_name;
    dl.file_size = file_meta.file_size;
    dl.chunk_size = file_meta.chunk_size > 0 ? file_meta.chunk_size : DEFAULT_CHUNK_SIZE;
    dl.total_chunks = file_meta.total_chunks;
    dl.expected_checksums = file_meta.chunk_checksums;

    // Clear any stale in-flight tasks for this file
    for (uint32_t c = 0; c < dl.total_chunks; ++c) {
        in_flight_.erase(FileManager::make_chunk_key(dl.file_id, c));
    }

    // If re-running 'get' on an already completed file, start a fresh download so demos can repeat
    if (!was_already_completed) {
        if (existing_it != downloads_.end()) {
            dl.completed_chunks = existing_it->second.completed_chunks;
        }

        for (uint32_t c = 0; c < dl.total_chunks; ++c) {
            std::vector<uint8_t> existing;
            if (file_mgr.read_chunk_bytes(dl.file_id, c, existing) && !existing.empty()) {
                if (c < dl.expected_checksums.size() && dl.expected_checksums[c] != 0) {
                    uint32_t csum = FileManager::compute_simple_checksum(existing.data(), existing.size());
                    if (csum == dl.expected_checksums[c]) {
                        dl.completed_chunks.insert(c);
                    }
                } else {
                    dl.completed_chunks.insert(c);
                }
            }
        }
    }

    if (dl.completed_chunks.size() == dl.total_chunks) {
        dl.completed = true;
        downloads_[dl.file_id] = dl;
        assemble_file(dl, file_mgr);
        return true;
    }

    dl.completed = false;
    downloads_[dl.file_id] = dl;
    return true;
}

std::vector<ChunkTask> DownloadManager::schedule_next_requests(
    uint32_t file_id,
    const FileManager& file_mgr,
    const std::unordered_map<uint32_t, std::pair<std::string, uint16_t>>& active_peer_endpoints,
    size_t max_new_requests
) {
    std::vector<ChunkTask> scheduled;
    auto dl_it = downloads_.find(file_id);
    if (dl_it == downloads_.end() || dl_it->second.completed) {
        return scheduled;
    }

    ActiveDownload& dl = dl_it->second;
    const auto& all_chunks = file_mgr.get_chunks();
    auto now = std::chrono::steady_clock::now();

    for (uint32_t c = 0; c < dl.total_chunks && scheduled.size() < max_new_requests; ++c) {
        if (dl.completed_chunks.count(c) > 0) {
            continue;
        }

        uint64_t ckey = FileManager::make_chunk_key(file_id, c);
        if (in_flight_.find(ckey) != in_flight_.end()) {
            continue;
        }

        auto cit = all_chunks.find(ckey);
        if (cit == all_chunks.end()) {
            continue;
        }

        std::vector<uint32_t> eligible_sources;
        for (uint32_t pid : cit->second.replica_peer_ids) {
            if (pid != self_id_ && active_peer_endpoints.find(pid) != active_peer_endpoints.end()) {
                eligible_sources.push_back(pid);
            }
        }

        if (eligible_sources.empty()) {
            continue;
        }

        uint32_t chosen_peer = eligible_sources[rr_source_cursor_ % eligible_sources.size()];
        rr_source_cursor_++;

        const auto& ep = active_peer_endpoints.at(chosen_peer);
        ChunkTask task;
        task.file_id = file_id;
        task.chunk_index = c;
        task.target_peer_id = chosen_peer;
        task.target_ip = ep.first;
        task.target_port = ep.second;
        task.sent_at = now;
        task.attempts = 1;

        in_flight_[ckey] = task;
        scheduled.push_back(task);
    }

    return scheduled;
}

void DownloadManager::handle_timeouts_and_peer_loss(const std::unordered_set<uint32_t>& active_peer_ids) {
    auto now = std::chrono::steady_clock::now();
    std::vector<uint64_t> to_release;

    for (const auto& kv : in_flight_) {
        const ChunkTask& task = kv.second;
        bool peer_dead = (active_peer_ids.find(task.target_peer_id) == active_peer_ids.end());
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - task.sent_at).count();

        if (peer_dead || elapsed_ms >= chunk_timeout_ms_) {
            to_release.push_back(kv.first);
        }
    }

    for (uint64_t key : to_release) {
        in_flight_.erase(key);
    }
}

bool DownloadManager::handle_chunk_received(
    uint32_t file_id,
    uint32_t chunk_index,
    uint32_t from_peer_id,
    const std::vector<uint8_t>& chunk_bytes,
    FileManager& file_mgr,
    bool& out_file_completed
) {
    out_file_completed = false;
    auto dl_it = downloads_.find(file_id);
    if (dl_it == downloads_.end()) {
        return false;
    }

    ActiveDownload& dl = dl_it->second;
    if (chunk_bytes.empty() || chunk_index >= dl.total_chunks) {
        return false;
    }

    if (chunk_index < dl.expected_checksums.size() && dl.expected_checksums[chunk_index] != 0) {
        uint32_t actual_csum = FileManager::compute_simple_checksum(chunk_bytes.data(), chunk_bytes.size());
        if (actual_csum != dl.expected_checksums[chunk_index]) {
            in_flight_.erase(FileManager::make_chunk_key(file_id, chunk_index));
            return false;
        }
    }

    if (!file_mgr.store_received_chunk(file_id, chunk_index, from_peer_id, chunk_bytes)) {
        return false;
    }

    in_flight_.erase(FileManager::make_chunk_key(file_id, chunk_index));
    dl.completed_chunks.insert(chunk_index);

    if (dl.completed_chunks.size() == dl.total_chunks) {
        dl.completed = true;
        if (assemble_file(dl, file_mgr)) {
            out_file_completed = true;
        }
    }

    return true;
}

bool DownloadManager::assemble_file(const ActiveDownload& dl, const FileManager& file_mgr) {
    std::error_code ec;
    fs::create_directories(data_dir_, ec);

    std::string out_path = data_dir_ + "/" + dl.file_name;
    std::ofstream ofs(out_path, std::ios::binary | std::ios::trunc);
    if (!ofs.is_open()) {
        return false;
    }

    uint64_t written_total = 0;
    for (uint32_t c = 0; c < dl.total_chunks; ++c) {
        std::vector<uint8_t> part;
        if (!file_mgr.read_chunk_bytes(dl.file_id, c, part)) {
            return false;
        }

        size_t to_write = part.size();
        if (dl.file_size > 0 && written_total + to_write > dl.file_size) {
            to_write = static_cast<size_t>(dl.file_size - written_total);
        }

        if (to_write > 0) {
            ofs.write(reinterpret_cast<const char*>(part.data()), static_cast<std::streamsize>(to_write));
            if (!ofs.good()) {
                return false;
            }
            written_total += to_write;
        }
    }

    return true;
}

bool DownloadManager::is_downloading(uint32_t file_id) const {
    auto it = downloads_.find(file_id);
    return it != downloads_.end() && !it->second.completed;
}

bool DownloadManager::is_completed(uint32_t file_id) const {
    auto it = downloads_.find(file_id);
    return it != downloads_.end() && it->second.completed;
}
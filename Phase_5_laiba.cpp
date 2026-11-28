#include "../include/file_manager.h"
#include <filesystem>
#include <fstream>
#include <sstream>
#include <algorithm>

namespace fs = std::filesystem;

// ==========================================
// PHASE 5: FILE CHUNKING & REPLICATION LOGIC
// ==========================================
FileManager::FileManager(uint32_t self_id, const std::string& data_dir, uint32_t chunk_size)
    : self_id_(self_id),
      data_dir_(data_dir),
      chunk_size_(chunk_size > 0 ? chunk_size : DEFAULT_CHUNK_SIZE) {}

uint64_t FileManager::make_chunk_key(uint32_t file_id, uint32_t chunk_index) {
    return (static_cast<uint64_t>(file_id) << 32) | static_cast<uint64_t>(chunk_index);
}

uint32_t FileManager::compute_simple_checksum(const uint8_t* data, size_t len) {
    uint32_t sum = 0x811c9dc5u; // FNV-1a style seed
    for (size_t i = 0; i < len; ++i) {
        sum ^= static_cast<uint32_t>(data[i]);
        sum *= 0x01000193u;
    }
    return sum;
}

uint32_t FileManager::generate_file_id(const std::string& filename) {
    uint32_t hash = 2166136261u;
    for (unsigned char c : filename) {
        hash ^= static_cast<uint32_t>(c);
        hash *= 16777619u;
    }
    return (hash == 0) ? 1u : hash;
}

std::string FileManager::chunk_storage_path(uint32_t file_id, uint32_t chunk_index) const {
    return data_dir_ + "/.chunks/f" + std::to_string(file_id) + "_c" + std::to_string(chunk_index) + ".bin";
}

bool FileManager::scan_local_directory() {
    std::error_code ec;
    if (!fs::exists(data_dir_, ec)) {
        fs::create_directories(data_dir_, ec);
    }
    fs::create_directories(data_dir_ + "/.chunks", ec);

    if (!fs::exists(data_dir_, ec) || !fs::is_directory(data_dir_, ec)) {
        return false;
    }

    for (const auto& entry : fs::directory_iterator(data_dir_, ec)) {
        if (!entry.is_regular_file(ec)) {
            continue;
        }

        std::string filename = entry.path().filename().string();
        if (!filename.empty() && filename[0] == '.') {
            continue; // Skip hidden/chunk cache files
        }

        uint64_t fsize = entry.file_size(ec);
        if (fsize == 0) {
            continue;
        }

        std::ifstream ifs(entry.path(), std::ios::binary);
        if (!ifs.is_open()) {
            continue;
        }

        uint32_t fid = generate_file_id(filename);
        uint32_t total_chunks = static_cast<uint32_t>((fsize + chunk_size_ - 1) / chunk_size_);

        FileMeta fmeta;
        fmeta.file_name = filename;
        fmeta.file_id = fid;
        fmeta.file_size = fsize;
        fmeta.chunk_size = chunk_size_;
        fmeta.total_chunks = total_chunks;

        std::vector<uint8_t> buf(chunk_size_);
        for (uint32_t c = 0; c < total_chunks; ++c) {
            ifs.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(chunk_size_));
            std::streamsize bytes_read = ifs.gcount();
            if (bytes_read <= 0) {
                break;
            }

            size_t u_read = static_cast<size_t>(bytes_read);
            uint32_t csum = compute_simple_checksum(buf.data(), u_read);
            fmeta.chunk_checksums.push_back(csum);

            uint64_t ckey = make_chunk_key(fid, c);
            ChunkMeta cmeta;
            cmeta.file_name = filename;
            cmeta.file_id = fid;
            cmeta.chunk_index = c;
            cmeta.total_chunks = total_chunks;
            cmeta.chunk_size = static_cast<uint32_t>(u_read);
            cmeta.checksum = csum;
            cmeta.replica_peer_ids.push_back(self_id_);

            chunks_[ckey] = cmeta;
            local_chunk_cache_[ckey] = std::vector<uint8_t>(buf.begin(), buf.begin() + u_read);
        }

        files_[fid] = fmeta;
    }

    return true;
}

bool FileManager::read_chunk_bytes(
    uint32_t file_id,
    uint32_t chunk_index,
    std::vector<uint8_t>& out_bytes
) const {
    uint64_t ckey = make_chunk_key(file_id, chunk_index);

    auto cache_it = local_chunk_cache_.find(ckey);
    if (cache_it != local_chunk_cache_.end()) {
        out_bytes = cache_it->second;
        return true;
    }

    std::string cpath = chunk_storage_path(file_id, chunk_index);
    std::ifstream ifs(cpath, std::ios::binary | std::ios::ate);
    if (ifs.is_open()) {
        std::streamsize sz = ifs.tellg();
        if (sz > 0) {
            ifs.seekg(0, std::ios::beg);
            out_bytes.resize(static_cast<size_t>(sz));
            ifs.read(reinterpret_cast<char*>(out_bytes.data()), sz);
            return ifs.good() || ifs.eof();
        }
    }

    auto file_it = files_.find(file_id);
    if (file_it == files_.end()) {
        return false;
    }

    std::string full_path = data_dir_ + "/" + file_it->second.file_name;
    std::ifstream f_ifs(full_path, std::ios::binary);
    if (!f_ifs.is_open()) {
        return false;
    }

    uint64_t offset = static_cast<uint64_t>(chunk_index) * file_it->second.chunk_size;
    f_ifs.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    out_bytes.resize(file_it->second.chunk_size);
    f_ifs.read(reinterpret_cast<char*>(out_bytes.data()), static_cast<std::streamsize>(file_it->second.chunk_size));
    std::streamsize got = f_ifs.gcount();
    if (got <= 0) {
        out_bytes.clear();
        return false;
    }

    out_bytes.resize(static_cast<size_t>(got));
    return true;
}

bool FileManager::store_received_chunk(
    uint32_t file_id,
    uint32_t chunk_index,
    uint32_t from_peer_id,
    const std::vector<uint8_t>& data
) {
    if (data.empty()) {
        return false;
    }

    std::error_code ec;
    fs::create_directories(data_dir_ + "/.chunks", ec);

    std::string cpath = chunk_storage_path(file_id, chunk_index);
    std::ofstream ofs(cpath, std::ios::binary | std::ios::trunc);
    if (!ofs.is_open()) {
        return false;
    }

    ofs.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
    if (!ofs.good()) {
        return false;
    }

    uint64_t ckey = make_chunk_key(file_id, chunk_index);
    local_chunk_cache_[ckey] = data;

    ChunkMeta& cmeta = chunks_[ckey];
    cmeta.file_id = file_id;
    cmeta.chunk_index = chunk_index;
    cmeta.chunk_size = static_cast<uint32_t>(data.size());
    cmeta.checksum = compute_simple_checksum(data.data(), data.size());

    if (std::find(cmeta.replica_peer_ids.begin(), cmeta.replica_peer_ids.end(), self_id_) == cmeta.replica_peer_ids.end()) {
        cmeta.replica_peer_ids.push_back(self_id_);
    }
    if (from_peer_id != 0 &&
        std::find(cmeta.replica_peer_ids.begin(), cmeta.replica_peer_ids.end(), from_peer_id) == cmeta.replica_peer_ids.end()) {
        cmeta.replica_peer_ids.push_back(from_peer_id);
    }

    return true;
}

void FileManager::distribute_replicas(const std::vector<uint32_t>& all_active_peer_ids) {
    if (all_active_peer_ids.empty()) {
        return;
    }

    // Round-robin assignment across peers so no single peer owns all chunks of a file
    size_t rr_idx = 0;
    for (auto& kv : chunks_) {
        ChunkMeta& cmeta = kv.second;
        size_t target_replicas = std::min(MIN_REPLICAS_PER_CHUNK, all_active_peer_ids.size());

        size_t safety_guard = 0;
        while (cmeta.replica_peer_ids.size() < target_replicas && safety_guard < all_active_peer_ids.size() * 2) {
            uint32_t candidate_id = all_active_peer_ids[rr_idx % all_active_peer_ids.size()];
            rr_idx++;
            safety_guard++;

            if (std::find(cmeta.replica_peer_ids.begin(), cmeta.replica_peer_ids.end(), candidate_id) ==
                cmeta.replica_peer_ids.end()) {
                cmeta.replica_peer_ids.push_back(candidate_id);
            }
        }
    }
}

void FileManager::merge_remote_metadata(
    uint32_t owner_peer_id,
    const FileMeta& file_meta,
    const std::vector<uint32_t>& available_chunk_indices
) {
    if (files_.find(file_meta.file_id) == files_.end()) {
        files_[file_meta.file_id] = file_meta;
    }

    for (uint32_t idx : available_chunk_indices) {
        uint64_t ckey = make_chunk_key(file_meta.file_id, idx);
        ChunkMeta& cmeta = chunks_[ckey];
        cmeta.file_name = file_meta.file_name;
        cmeta.file_id = file_meta.file_id;
        cmeta.chunk_index = idx;
        cmeta.total_chunks = file_meta.total_chunks;
        cmeta.chunk_size = file_meta.chunk_size;
        if (idx < file_meta.chunk_checksums.size()) {
            cmeta.checksum = file_meta.chunk_checksums[idx];
        }

        if (owner_peer_id != 0 &&
            std::find(cmeta.replica_peer_ids.begin(), cmeta.replica_peer_ids.end(), owner_peer_id) ==
                cmeta.replica_peer_ids.end()) {
            cmeta.replica_peer_ids.push_back(owner_peer_id);
        }
    }
}

std::string FileManager::export_metadata_payload() const {
    // Format per line: FILE|<file_id>|<file_name>|<file_size>|<chunk_size>|<total_chunks>|<c0,c1,...>
    std::ostringstream oss;
    for (const auto& fkv : files_) {
        const FileMeta& fm = fkv.second;
        std::vector<uint32_t> owned_indices;
        for (uint32_t c = 0; c < fm.total_chunks; ++c) {
            uint64_t ckey = make_chunk_key(fm.file_id, c);
            auto cit = chunks_.find(ckey);
            if (cit != chunks_.end() &&
                std::find(cit->second.replica_peer_ids.begin(), cit->second.replica_peer_ids.end(), self_id_) !=
                    cit->second.replica_peer_ids.end()) {
                owned_indices.push_back(c);
            }
        }

        oss << "FILE|" << fm.file_id << "|" << fm.file_name << "|"
            << fm.file_size << "|" << fm.chunk_size << "|" << fm.total_chunks << "|";
        for (size_t i = 0; i < owned_indices.size(); ++i) {
            if (i > 0) oss << ",";
            oss << owned_indices[i];
        }
        oss << "\n";
    }
    return oss.str();
}

void FileManager::import_metadata_payload(uint32_t from_peer_id, const std::string& payload) {
    std::istringstream iss(payload);
    std::string line;
    while (std::getline(iss, line)) {
        if (line.empty()) continue;

        std::vector<std::string> parts;
        std::stringstream ls(line);
        std::string token;
        while (std::getline(ls, token, '|')) {
            parts.push_back(token);
        }

        if (parts.size() < 7 || parts[0] != "FILE") {
            continue;
        }

        try {
            FileMeta fm;
            fm.file_id = static_cast<uint32_t>(std::stoul(parts[1]));
            fm.file_name = parts[2];
            fm.file_size = std::stoull(parts[3]);
            fm.chunk_size = static_cast<uint32_t>(std::stoul(parts[4]));
            fm.total_chunks = static_cast<uint32_t>(std::stoul(parts[5]));

            std::vector<uint32_t> indices;
            if (!parts[6].empty()) {
                std::stringstream cs(parts[6]);
                std::string idx_str;
                while (std::getline(cs, idx_str, ',')) {
                    if (!idx_str.empty()) {
                        indices.push_back(static_cast<uint32_t>(std::stoul(idx_str)));
                    }
                }
            }

            merge_remote_metadata(from_peer_id, fm, indices);
        } catch (...) {
            continue;
        }
    }
}

const std::unordered_map<uint32_t, FileMeta>& FileManager::get_files() const {
    return files_;
}

const std::unordered_map<uint64_t, ChunkMeta>& FileManager::get_chunks() const {
    return chunks_;
}

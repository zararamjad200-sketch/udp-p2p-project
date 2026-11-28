#ifndef FILE_MANAGER_H
#define FILE_MANAGER_H

#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>


// PHASE 5

constexpr uint32_t DEFAULT_CHUNK_SIZE = 8192; // 64 KB chunks
constexpr size_t MIN_REPLICAS_PER_CHUNK = 2;

struct ChunkMeta {
    std::string file_name;
    uint32_t file_id = 0;
    uint32_t chunk_index = 0;
    uint32_t total_chunks = 0;
    uint32_t chunk_size = 0;
    uint32_t checksum = 0;
    std::vector<uint32_t> replica_peer_ids; // Peers holding this chunk
};

struct FileMeta {
    std::string file_name;
    uint32_t file_id = 0;
    uint64_t file_size = 0;
    uint32_t chunk_size = DEFAULT_CHUNK_SIZE;
    uint32_t total_chunks = 0;
    std::vector<uint32_t> chunk_checksums;
};

class FileManager {
public:
    explicit FileManager(
        uint32_t self_id,
        const std::string& data_dir,
        uint32_t chunk_size = DEFAULT_CHUNK_SIZE
    );

    // Scans data_dir and registers local files + chunks
    bool scan_local_directory();

    // Reads a specific chunk's raw bytes from disk (local file or stored chunk)
    bool read_chunk_bytes(
        uint32_t file_id,
        uint32_t chunk_index,
        std::vector<uint8_t>& out_bytes
    ) const;

    // Writes a received chunk's raw bytes to disk and updates replica ownership
    bool store_received_chunk(
        uint32_t file_id,
        uint32_t chunk_index,
        uint32_t from_peer_id,
        const std::vector<uint8_t>& data
    );

    // Plans/assigns at least MIN_REPLICAS_PER_CHUNK replicas across active peers
    // (including self_id) without letting one peer hold an entire file alone
    void distribute_replicas(const std::vector<uint32_t>& all_active_peer_ids);

    // Registers or merges remote file/chunk metadata received on peer join/sync
    void merge_remote_metadata(
        uint32_t owner_peer_id,
        const FileMeta& file_meta,
        const std::vector<uint32_t>& available_chunk_indices
    );

    // Serializes/deserializes compact metadata lines for join/sync exchange
    std::string export_metadata_payload() const;
    void import_metadata_payload(uint32_t from_peer_id, const std::string& payload);

    const std::unordered_map<uint32_t, FileMeta>& get_files() const;
    const std::unordered_map<uint64_t, ChunkMeta>& get_chunks() const;

    static uint64_t make_chunk_key(uint32_t file_id, uint32_t chunk_index);
    static uint32_t compute_simple_checksum(const uint8_t* data, size_t len);

private:
    uint32_t self_id_;
    std::string data_dir_;
    uint32_t chunk_size_;

    std::unordered_map<uint32_t, FileMeta> files_;
    std::unordered_map<uint64_t, ChunkMeta> chunks_;
    std::unordered_map<uint64_t, std::vector<uint8_t>> local_chunk_cache_;

    static uint32_t generate_file_id(const std::string& filename);
    std::string chunk_storage_path(uint32_t file_id, uint32_t chunk_index) const;
};

#endif // FILE_MANAGER_H
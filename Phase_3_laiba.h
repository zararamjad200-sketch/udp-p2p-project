#ifndef PEER_MANAGER_H
#define PEER_MANAGER_H

#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>
#include <chrono>


// PHASE 3

struct PeerInfo {
    uint32_t peer_id = 0;
    std::string ip;
    uint16_t port = 0;
    bool active = true;
    std::chrono::steady_clock::time_point last_seen;
};

class PeerManager {
public:
    // heartbeat_timeout_sec: time without heartbeat before peer is marked inactive
    explicit PeerManager(uint32_t self_id, int heartbeat_timeout_sec = 6);

    // Adds a new peer or refreshes an existing peer's last_seen timestamp
    bool upsert_peer(uint32_t peer_id, const std::string& ip, uint16_t port);

    // Marks a peer as inactive when a LEAVE packet is received
    bool mark_peer_left(uint32_t peer_id);

    // Scans peers and marks any peer exceeding timeout as inactive; returns newly timed-out IDs
    std::vector<uint32_t> check_timeouts();

    // Returns a snapshot of currently active peers (excluding self_id)
    std::vector<PeerInfo> get_active_peers() const;

    // Checks if a peer ID is currently active
    bool is_peer_active(uint32_t peer_id) const;

    // Encodes "ip:port" payload for JOIN / peer exchange messages
    static std::string encode_peer_endpoint(const std::string& ip, uint16_t port);

    // Decodes "ip:port" payload; returns false if malformed
    static bool decode_peer_endpoint(const std::string& payload, std::string& out_ip, uint16_t& out_port);

private:
    uint32_t self_id_;
    int timeout_sec_;
    std::unordered_map<uint32_t, PeerInfo> peers_;   //key-value pairs
};

#endif // PEER_MANAGER_H
#include "../include/peer_manager.h"
#include <cstdlib>

PeerManager::PeerManager(uint32_t self_id, int heartbeat_timeout_sec)
    : self_id_(self_id),
      timeout_sec_(heartbeat_timeout_sec > 0 ? heartbeat_timeout_sec : 6) {}

bool PeerManager::upsert_peer(uint32_t peer_id, const std::string& ip, uint16_t port) {
    if (peer_id == 0 || peer_id == self_id_ || ip.empty() || port == 0) {
        return false;
    }

    auto now = std::chrono::steady_clock::now();
    auto it = peers_.find(peer_id);
    if (it == peers_.end()) {
        PeerInfo info;
        info.peer_id = peer_id;
        info.ip = ip;
        info.port = port;
        info.active = true;
        info.last_seen = now;
        peers_[peer_id] = info;
    } else {
        it->second.ip = ip;
        it->second.port = port;
        it->second.active = true;
        it->second.last_seen = now;
    }
    return true;
}

bool PeerManager::mark_peer_left(uint32_t peer_id) {
    auto it = peers_.find(peer_id);
    if (it == peers_.end()) {
        return false;
    }
    it->second.active = false;
    return true;
}

std::vector<uint32_t> PeerManager::check_timeouts() {
    std::vector<uint32_t> timed_out;
    auto now = std::chrono::steady_clock::now();

    for (auto& kv : peers_) {
        PeerInfo& p = kv.second;
        if (!p.active) {
            continue;
        }
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - p.last_seen).count();
        if (elapsed >= timeout_sec_) {
            p.active = false;
            timed_out.push_back(p.peer_id);
        }
    }
    return timed_out;
}

std::vector<PeerInfo> PeerManager::get_active_peers() const {
    std::vector<PeerInfo> result;
    for (const auto& kv : peers_) {
        if (kv.second.active && kv.second.peer_id != self_id_) {
            result.push_back(kv.second);
        }
    }
    return result;
}

bool PeerManager::is_peer_active(uint32_t peer_id) const {
    auto it = peers_.find(peer_id);
    return it != peers_.end() && it->second.active;
}

std::string PeerManager::encode_peer_endpoint(const std::string& ip, uint16_t port) {
    return ip + ":" + std::to_string(port);
}

bool PeerManager::decode_peer_endpoint(const std::string& payload, std::string& out_ip, uint16_t& out_port) {
    size_t colon = payload.rfind(':');
    if (colon == std::string::npos || colon == 0 || colon + 1 >= payload.size()) {
        return false;
    }
    out_ip = payload.substr(0, colon);
    char* end_ptr = nullptr;
    long val = std::strtol(payload.substr(colon + 1).c_str(), &end_ptr, 10);
    if (end_ptr == nullptr || *end_ptr != '\0' || val <= 0 || val > 65535) {
        return false;
    }
    out_port = static_cast<uint16_t>(val);
    return true;
}

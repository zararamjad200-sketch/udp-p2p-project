#include "../include/p2p_engine.h"
#include "../include/socket_utils.h"
#include <iostream>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <thread>
#include <filesystem>
#include <unordered_set>
#include <unordered_map>
#include <algorithm>
#include <cctype>
#include <unistd.h>
#include <fcntl.h>

namespace fs = std::filesystem;

namespace {
bool is_all_digits(const std::string& s) {
    return !s.empty() && std::all_of(s.begin(), s.end(), [](unsigned char c) {
        return std::isdigit(c) != 0;
    });
}

std::string trim_copy(const std::string& s) {
    size_t start = 0;
    while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) {
        ++start;
    }
    size_t end = s.size();
    while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) {
        --end;
    }
    return s.substr(start, end - start);
}

std::string format_peer_list(const std::vector<uint32_t>& ids) {
    if (ids.empty()) {
        return "none";
    }
    std::ostringstream oss;
    for (size_t i = 0; i < ids.size(); ++i) {
        if (i > 0) oss << ", ";
        oss << "Peer " << ids[i];
    }
    return oss.str();
}
} // namespace

P2PEngine::P2PEngine(
    const EngineConfig& cfg,
    int udp_sock,
    PeerManager& peer_mgr,
    ChatManager& chat_mgr,
    FileManager& file_mgr,
    DownloadManager& download_mgr,
    FaultManager& fault_mgr
)
    : cfg_(cfg),
      udp_sock_(udp_sock),
      peer_mgr_(peer_mgr),
      chat_mgr_(chat_mgr),
      file_mgr_(file_mgr),
      download_mgr_(download_mgr),
      fault_mgr_(fault_mgr) {}

void P2PEngine::send_raw_packet(
    const std::string& ip,
    uint16_t port,
    MessageType type,
    uint32_t seq_num,
    const std::string& payload
) {
    if (ip.empty() || port == 0) {
        return;
    }
    std::vector<uint8_t> raw = serialize_packet(type, cfg_.self_id, seq_num, payload, 0);
    send_udp_to(udp_sock_, ip, port, raw.data(), raw.size());
}

void P2PEngine::send_binary_packet_to(
    const std::string& ip,
    uint16_t port,
    MessageType type,
    uint32_t seq_num,
    const uint8_t* data,
    uint32_t len
) {
    if (ip.empty() || port == 0) {
        return;
    }
    std::vector<uint8_t> raw = serialize_binary_packet(type, cfg_.self_id, seq_num, data, len, 0);
    send_udp_to(udp_sock_, ip, port, raw.data(), raw.size());
}

void P2PEngine::send_join_to_bootstrap() {
    if (cfg_.bootstrap_ip.empty() || cfg_.bootstrap_port == 0) {
        return;
    }
    std::string self_ep = PeerManager::encode_peer_endpoint(cfg_.bind_ip, cfg_.port);
    send_raw_packet(cfg_.bootstrap_ip, cfg_.bootstrap_port, MessageType::JOIN, 0, self_ep);

    std::string meta_payload = file_mgr_.export_metadata_payload();
    if (!meta_payload.empty()) {
        send_raw_packet(cfg_.bootstrap_ip, cfg_.bootstrap_port, MessageType::CHAT, 0, "__META__\n" + meta_payload);
    }
}

void P2PEngine::broadcast_chat(const std::string& text) {
    uint32_t seq = chat_mgr_.next_outgoing_seq();
    std::vector<PeerInfo> active = peer_mgr_.get_active_peers();

    for (const auto& p : active) {
        send_raw_packet(p.ip, p.port, MessageType::CHAT, seq, text);
        chat_mgr_.track_outgoing_message(p.peer_id, p.ip, p.port, seq, text);
    }
}

void P2PEngine::process_incoming_datagrams() {
    for (int budget = 0; budget < 256; ++budget) {
        std::vector<uint8_t> buf(70000);
        std::string src_ip;
        uint16_t src_port = 0;
        ssize_t n = recv_udp_from(udp_sock_, buf.data(), buf.size(), src_ip, src_port);
        if (n <= 0) {
            break;
        }

        PacketHeader hdr{};
        if (!deserialize_header(buf.data(), static_cast<size_t>(n), hdr)) {
            continue;
        }

        if (hdr.sender_id == cfg_.self_id) {
            continue;
        }

        bool was_inactive = !peer_mgr_.is_peer_active(hdr.sender_id);
        peer_mgr_.upsert_peer(hdr.sender_id, src_ip, src_port);

        const uint8_t* payload_ptr = buf.data() + PACKET_HEADER_SIZE;
        std::string payload(
            reinterpret_cast<const char*>(payload_ptr),
            static_cast<size_t>(hdr.payload_len)
        );

        MessageType mtype = static_cast<MessageType>(hdr.msg_type);

        switch (mtype) {
            case MessageType::JOIN: {
                std::string ep_ip;
                uint16_t ep_port = 0;
                if (PeerManager::decode_peer_endpoint(payload, ep_ip, ep_port)) {
                    peer_mgr_.upsert_peer(hdr.sender_id, ep_ip, ep_port);
                }
                if (was_inactive) {
                    std::cout << "\n[PEER JOIN] Peer " << hdr.sender_id
                              << " connected/rejoined (" << src_ip << ":" << src_port << ")\n";
                }
                std::string meta_payload = file_mgr_.export_metadata_payload();
                if (!meta_payload.empty()) {
                    send_raw_packet(src_ip, src_port, MessageType::CHAT, 0, "__META__\n" + meta_payload);
                }
                if (active_download_file_id_ != 0) {
                    std::cout << "\n[RESUME READY] Peer " << hdr.sender_id
                              << " online -> resuming remaining chunks for File ID="
                              << active_download_file_id_ << "\n";
                }
                break;
            }
            case MessageType::LEAVE: {
                peer_mgr_.mark_peer_left(hdr.sender_id);
                std::unordered_set<uint32_t> alive_ids;
                std::vector<uint32_t> alive_vec;
                for (const auto& p : peer_mgr_.get_active_peers()) {
                    alive_ids.insert(p.peer_id);
                    alive_vec.push_back(p.peer_id);
                }
                fault_mgr_.handle_peer_failure(hdr.sender_id, alive_ids, file_mgr_, chat_mgr_, download_mgr_);
                std::cout << "\n[FAILOVER] Peer " << hdr.sender_id
                          << " left -> active peers: " << format_peer_list(alive_vec) << "\n";
                break;
            }
            case MessageType::HEARTBEAT: {
                if (was_inactive) {
                    std::cout << "\n[PEER RECOVERED] Peer " << hdr.sender_id
                              << " heartbeat active (" << src_ip << ":" << src_port << ")\n";
                }
                break;
            }
            case MessageType::CHAT: {
                const std::string meta_prefix = "__META__\n";
                if (payload.rfind(meta_prefix, 0) == 0) {
                    size_t before_count = file_mgr_.get_files().size();
                    file_mgr_.import_metadata_payload(hdr.sender_id, payload.substr(meta_prefix.size()));
                    if (file_mgr_.get_files().size() > before_count) {
                        std::cout << "\n[FILE SYNC] Metadata received from Peer "
                                  << hdr.sender_id << " (type 'files' to inspect chunks & replicas)\n";
                    }
                    break;
                }
                auto ready_msgs = chat_mgr_.handle_incoming_chat(
                    hdr.sender_id, src_ip, src_port, hdr.seq_num, payload
                );
                for (const auto& m : ready_msgs) {
                    std::cout << "\n[CHAT <- Peer " << m.first << "]: " << m.second << "\n";
                }
                break;
            }
            case MessageType::ACK: {
                chat_mgr_.handle_ack(hdr.sender_id, hdr.seq_num);
                break;
            }
            case MessageType::CHUNK_REQUEST: {
                size_t colon = payload.find(':');
                if (colon != std::string::npos) {
                    try {
                        uint32_t fid = static_cast<uint32_t>(std::stoul(payload.substr(0, colon)));
                        uint32_t cidx = static_cast<uint32_t>(std::stoul(payload.substr(colon + 1)));
                        std::vector<uint8_t> chunk_bytes;
                        if (file_mgr_.read_chunk_bytes(fid, cidx, chunk_bytes) && !chunk_bytes.empty()) {
                            std::cout << "\n[CHUNK SERVED -> Peer " << hdr.sender_id
                                      << "] File=" << fid << " Chunk #" << cidx << "\n";
                            send_binary_packet_to(
                                src_ip,
                                src_port,
                                MessageType::CHUNK_DATA,
                                cidx,
                                chunk_bytes.data(),
                                static_cast<uint32_t>(chunk_bytes.size())
                            );
                            std::this_thread::sleep_for(std::chrono::milliseconds(15));
                        }
                    } catch (...) {
                    }
                }
                break;
            }
            case MessageType::CHUNK_DATA: {
                if (active_download_file_id_ != 0) {
                    std::vector<uint8_t> chunk_bytes(payload_ptr, payload_ptr + hdr.payload_len);
                    bool done = false;
                    if (download_mgr_.handle_chunk_received(
                            active_download_file_id_, hdr.seq_num, hdr.sender_id, chunk_bytes, file_mgr_, done
                        )) {
                        received_chunks_count_++;
                        auto fit = file_mgr_.get_files().find(active_download_file_id_);
                        std::string fname = (fit != file_mgr_.get_files().end())
                            ? fit->second.file_name
                            : std::to_string(active_download_file_id_);
                        uint32_t tot = (fit != file_mgr_.get_files().end() && fit->second.total_chunks > 0)
                            ? fit->second.total_chunks
                            : received_chunks_count_;
                        int pct = static_cast<int>((std::min(received_chunks_count_, tot) * 100) / tot);

                        std::cout << "\n[CHUNK <- Peer " << hdr.sender_id << "] Chunk #" << hdr.seq_num
                                  << " | Progress: " << received_chunks_count_ << "/" << tot << " (" << pct << "%)\n";

                        std::string meta_payload = file_mgr_.export_metadata_payload();
                        if (!meta_payload.empty()) {
                            for (const auto& p : peer_mgr_.get_active_peers()) {
                                send_raw_packet(p.ip, p.port, MessageType::CHAT, 0, "__META__\n" + meta_payload);
                            }
                        }

                        if (done) {
                            std::cout << "========================================\n"
                                      << "[DOWNLOAD COMPLETE] " << fname << " (ID=" << active_download_file_id_ << ")\n"
                                      << "Chunks Received : " << tot << "/" << tot << " (100%)\n"
                                      << "Saved To        : " << cfg_.data_dir << "/" << fname << "\n"
                                      << "========================================\n";
                            active_download_file_id_ = 0;
                            received_chunks_count_ = 0;
                        }
                    }
                }
                break;
            }
            default:
                break;
        }
    }
}

void P2PEngine::process_periodic_ticks() {
    static auto last_tick = std::chrono::steady_clock::now();
    static auto last_progress_print = std::chrono::steady_clock::now();

    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_tick).count();
    if (elapsed < 150) {
        return;
    }
    last_tick = now;

    auto timed_out = peer_mgr_.check_timeouts();
    std::vector<PeerInfo> active_peers = peer_mgr_.get_active_peers();

    std::unordered_set<uint32_t> alive_set;
    std::vector<uint32_t> alive_vec;
    std::unordered_map<uint32_t, std::pair<std::string, uint16_t>> alive_endpoints;

    for (const auto& p : active_peers) {
        alive_set.insert(p.peer_id);
        alive_vec.push_back(p.peer_id);
        alive_endpoints[p.peer_id] = {p.ip, p.port};
    }

    for (uint32_t dead_id : timed_out) {
        fault_mgr_.handle_peer_failure(dead_id, alive_set, file_mgr_, chat_mgr_, download_mgr_);
        std::cout << "\n[FAILOVER] Peer " << dead_id
                  << " offline (heartbeat timeout) -> active peers: "
                  << format_peer_list(alive_vec) << "\n";
    }

    if (!alive_vec.empty()) {
        file_mgr_.distribute_replicas(alive_vec);
        fault_mgr_.plan_re_replication(file_mgr_, alive_vec, MIN_REPLICAS_PER_CHUNK);
    }

    for (const auto& p : active_peers) {
        send_raw_packet(p.ip, p.port, MessageType::HEARTBEAT, 0, "");
    }

    for (const auto& ack : chat_mgr_.pop_outbound_acks()) {
        send_raw_packet(ack.target_ip, ack.target_port, MessageType::ACK, ack.ack_seq_num, "");
    }
    for (const auto& retry_msg : chat_mgr_.collect_retries()) {
        send_raw_packet(retry_msg.target_ip, retry_msg.target_port, MessageType::CHAT, retry_msg.seq_num, retry_msg.payload);
    }

    if (active_download_file_id_ != 0) {
        download_mgr_.handle_timeouts_and_peer_loss(alive_set);
        auto tasks = download_mgr_.schedule_next_requests(
            active_download_file_id_, file_mgr_, alive_endpoints, 2
        );
        for (const auto& t : tasks) {
            std::string req_payload = std::to_string(t.file_id) + ":" + std::to_string(t.chunk_index);
            send_raw_packet(t.target_ip, t.target_port, MessageType::CHUNK_REQUEST, t.chunk_index, req_payload);
        }

        auto prog_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_progress_print).count();
        if (prog_elapsed >= 1500) {
            last_progress_print = now;
            auto fit = file_mgr_.get_files().find(active_download_file_id_);
            uint32_t total = (fit != file_mgr_.get_files().end() && fit->second.total_chunks > 0)
                ? fit->second.total_chunks
                : 1;
            uint32_t done_cnt = std::min(received_chunks_count_, total);
            uint32_t rem_cnt = (total > done_cnt) ? (total - done_cnt) : 0;
            int pct = static_cast<int>((done_cnt * 100) / total);
            std::string fname = (fit != file_mgr_.get_files().end()) ? fit->second.file_name : "file";

            if (tasks.empty() && rem_cnt > 0) {
                std::cout << "\n[NO REPLICA AVAILABLE] " << fname
                          << " | Saved: " << done_cnt << "/" << total << " (" << pct << "%)"
                          << " | Remaining: " << rem_cnt
                          << " | Waiting for replica peer(s) to come online...\n";
            } else {
                std::cout << "\n[PROGRESS] " << fname
                          << " | " << done_cnt << "/" << total << " chunks (" << pct << "%)"
                          << " | Remaining: " << rem_cnt
                          << " | Active Peers: " << format_peer_list(alive_vec) << "\n";
            }
        }
    }
}

void P2PEngine::execute_command(const std::string& raw_line, bool& should_exit) {
    std::string line = trim_copy(raw_line);
    if (line.empty()) {
        return;
    }

    if (line == "quit" || line == "exit") {
        for (const auto& p : peer_mgr_.get_active_peers()) {
            send_raw_packet(p.ip, p.port, MessageType::LEAVE, 0, "");
        }
        std::cout << "[SHUTDOWN] Peer " << cfg_.self_id << " leaving network...\n";
        should_exit = true;
        return;
    }

    if (line == "peers") {
        auto active = peer_mgr_.get_active_peers();
        std::cout << "\n--- Active Peers (" << active.size() << ") ---\n";
        for (const auto& p : active) {
            std::cout << "  * Peer " << p.peer_id << " @ " << p.ip << ":" << p.port << " [ALIVE]\n";
        }
        return;
    }

    if (line == "files") {
        std::cout << "\n--- Distributed Files, Chunks & Replicas ---\n";
        for (const auto& kv : file_mgr_.get_files()) {
            const FileMeta& fm = kv.second;
            std::cout << "  File ID : " << fm.file_id
                      << " | Name: " << fm.file_name
                      << " | Size: " << fm.file_size << " B"
                      << " | Chunks: " << fm.total_chunks << "\n";

            for (uint32_t c = 0; c < fm.total_chunks; ++c) {
                uint64_t ckey = FileManager::make_chunk_key(fm.file_id, c);
                auto cit = file_mgr_.get_chunks().find(ckey);
                std::string rep_str = (cit != file_mgr_.get_chunks().end())
                    ? format_peer_list(cit->second.replica_peer_ids)
                    : "pending sync";
                std::cout << "      Chunk " << c << " -> Replicas: [" << rep_str << "]\n";
            }
        }
        return;
    }

    if (line.rfind("upload ", 0) == 0) {
        std::string src_path = trim_copy(line.substr(7));
        std::error_code ec;
        if (src_path.empty() || !fs::exists(src_path, ec) || !fs::is_regular_file(src_path, ec)) {
            std::cout << "[ERROR] Upload file not found: '" << src_path << "'\n";
            return;
        }
        fs::create_directories(cfg_.data_dir, ec);
        fs::path dest = fs::path(cfg_.data_dir) / fs::path(src_path).filename();
        fs::copy_file(src_path, dest, fs::copy_options::overwrite_existing, ec);
        if (ec) {
            std::cout << "[ERROR] Failed copying file to " << cfg_.data_dir << ": " << ec.message() << "\n";
            return;
        }
        file_mgr_.scan_local_directory();
        std::string meta_payload = file_mgr_.export_metadata_payload();
        for (const auto& p : peer_mgr_.get_active_peers()) {
            send_raw_packet(p.ip, p.port, MessageType::CHAT, 0, "__META__\n" + meta_payload);
        }
        std::cout << "[UPLOADED & SYNCED] '" << dest.filename().string()
                  << "' added to " << cfg_.data_dir << " and metadata broadcasted to active peers.\n";
        return;
    }

    if (line.rfind("chat ", 0) == 0) {
        std::string msg = line.substr(5);
        broadcast_chat(msg);
        std::cout << "[CHAT -> ALL]: " << msg << "\n";
        return;
    }

    if (line.rfind("get ", 0) == 0) {
        std::string target = trim_copy(line.substr(4));
        const auto& files = file_mgr_.get_files();
        const FileMeta* matched = nullptr;

        if (is_all_digits(target)) {
            try {
                uint32_t fid = static_cast<uint32_t>(std::stoul(target));
                auto it = files.find(fid);
                if (it != files.end()) {
                    matched = &it->second;
                }
            } catch (...) {
                matched = nullptr;
            }
        } else {
            for (const auto& kv : files) {
                if (kv.second.file_name == target) {
                    matched = &kv.second;
                    break;
                }
            }
        }

        if (matched == nullptr) {
            std::cout << "[ERROR] File not found: '" << target << "' (use ID or exact file name)\n";
            return;
        }

        if (download_mgr_.start_download(*matched, file_mgr_)) {
            active_download_file_id_ = matched->file_id;
            received_chunks_count_ = 0;
            std::cout << "\n[DOWNLOAD STARTED] " << matched->file_name
                      << " (ID=" << matched->file_id << ", Total Chunks=" << matched->total_chunks << ")\n";

            std::unordered_set<uint32_t> alive_set;
            std::unordered_map<uint32_t, std::pair<std::string, uint16_t>> alive_endpoints;
            for (const auto& p : peer_mgr_.get_active_peers()) {
                alive_set.insert(p.peer_id);
                alive_endpoints[p.peer_id] = {p.ip, p.port};
            }
            download_mgr_.handle_timeouts_and_peer_loss(alive_set);
            auto tasks = download_mgr_.schedule_next_requests(
                active_download_file_id_, file_mgr_, alive_endpoints, 2
            );
            std::cout << "[SEND REQ] Scheduled " << tasks.size() << " chunk request(s)\n";
            for (const auto& t : tasks) {
                std::string req_payload = std::to_string(t.file_id) + ":" + std::to_string(t.chunk_index);
                send_raw_packet(t.target_ip, t.target_port, MessageType::CHUNK_REQUEST, t.chunk_index, req_payload);
            }
        }
        return;
    }

    std::cout << "Commands: peers | files | upload <path> | chat <text> | get <id_or_name> | quit\n";
}

void P2PEngine::process_stdin_commands(bool& should_exit) {
    int flags = fcntl(STDIN_FILENO, F_GETFL, 0);
    if (flags >= 0 && (flags & O_NONBLOCK) == 0) {
        fcntl(STDIN_FILENO, F_SETFL, flags | O_NONBLOCK);
    }

    char ch = 0;
    while (read(STDIN_FILENO, &ch, 1) > 0) {
        if (ch == '\n') {
            execute_command(stdin_line_buf_, should_exit);
            stdin_line_buf_.clear();
            if (should_exit) {
                return;
            }
        } else if (ch != '\r') {
            stdin_line_buf_.push_back(ch);
        }
    }
}

void P2PEngine::run() {
    file_mgr_.scan_local_directory();
    std::cout << "========================================\n"
              << "  Peer " << cfg_.self_id << " Online on " << cfg_.bind_ip << ":" << cfg_.port << "\n"
              << "  Data Dir : " << cfg_.data_dir << "\n"
              << "  Files    : " << file_mgr_.get_files().size() << " local file(s) indexed\n"
              << "========================================\n";

    send_join_to_bootstrap();

    bool should_exit = false;
    while (!should_exit) {
        process_incoming_datagrams();
        process_periodic_ticks();
        process_stdin_commands(should_exit);
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
}
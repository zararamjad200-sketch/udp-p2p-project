#ifndef P2P_ENGINE_H
#define P2P_ENGINE_H

#include <cstdint>
#include <string>
#include "protocol.h"
#include "peer_manager.h"
#include "chat_manager.h"
#include "file_manager.h"
#include "download_manager.h"
#include "fault_manager.h"

struct EngineConfig {
    uint32_t self_id = 1;
    std::string bind_ip = "127.0.0.1";
    uint16_t port = 5001;
    std::string data_dir = "./peer_data";
    std::string bootstrap_ip;
    uint16_t bootstrap_port = 0;
};

class P2PEngine {
public:
    P2PEngine(
        const EngineConfig& cfg,
        int udp_sock,
        PeerManager& peer_mgr,
        ChatManager& chat_mgr,
        FileManager& file_mgr,
        DownloadManager& download_mgr,
        FaultManager& fault_mgr
    );

    void run();

private:
    EngineConfig cfg_;
    int udp_sock_;
    PeerManager& peer_mgr_;
    ChatManager& chat_mgr_;
    FileManager& file_mgr_;
    DownloadManager& download_mgr_;
    FaultManager& fault_mgr_;

    uint32_t active_download_file_id_ = 0;
    uint32_t received_chunks_count_ = 0;
    std::string stdin_line_buf_;
    void send_raw_packet(
        const std::string& ip,
        uint16_t port,
        MessageType type,
        uint32_t seq_num,
        const std::string& payload
    );

    void send_binary_packet_to(
        const std::string& ip,
        uint16_t port,
        MessageType type,
        uint32_t seq_num,
        const uint8_t* data,
        uint32_t len
    );

    void send_join_to_bootstrap();
    void broadcast_chat(const std::string& text);
    void process_incoming_datagrams();
    void process_periodic_ticks();
    void process_stdin_commands(bool& should_exit);
    void execute_command(const std::string& line, bool& should_exit);
};

#endif // P2P_ENGINE_H
#include "../include/protocol.h"

std::vector<uint8_t> serialize_binary_packet(
    MessageType type,
    uint32_t sender_id,
    uint32_t seq_num,
    const uint8_t* payload_data,
    uint32_t payload_len,
    uint16_t flags
) {
    PacketHeader net_hdr{};
    net_hdr.version = PROTOCOL_VERSION;
    net_hdr.msg_type = static_cast<uint8_t>(type);
    net_hdr.flags = htons(flags);
    net_hdr.sender_id = htonl(sender_id);
    net_hdr.seq_num = htonl(seq_num);
    net_hdr.payload_len = htonl(payload_len);

    std::vector<uint8_t> out(PACKET_HEADER_SIZE + static_cast<size_t>(payload_len));
    std::memcpy(out.data(), &net_hdr, PACKET_HEADER_SIZE);

    if (payload_len > 0 && payload_data != nullptr) {
        std::memcpy(out.data() + PACKET_HEADER_SIZE, payload_data, static_cast<size_t>(payload_len));
    }

    return out;
}

std::vector<uint8_t> serialize_packet(
    MessageType type,
    uint32_t sender_id,
    uint32_t seq_num,
    const std::string& payload,
    uint16_t flags
) {
    const uint8_t* ptr = payload.empty()
        ? nullptr
        : reinterpret_cast<const uint8_t*>(payload.data());
    return serialize_binary_packet(
        type,
        sender_id,
        seq_num,
        ptr,
        static_cast<uint32_t>(payload.size()),
        flags
    );
}

bool deserialize_header(
    const uint8_t* buffer,
    size_t buffer_size,
    PacketHeader& out_host_header
) {
    if (buffer == nullptr || buffer_size < PACKET_HEADER_SIZE) {
        return false;
    }

    PacketHeader net_hdr{};
    std::memcpy(&net_hdr, buffer, PACKET_HEADER_SIZE);

    if (net_hdr.version != PROTOCOL_VERSION) {
        return false;
    }

    out_host_header.version = net_hdr.version;
    out_host_header.msg_type = net_hdr.msg_type;
    out_host_header.flags = ntohs(net_hdr.flags);
    out_host_header.sender_id = ntohl(net_hdr.sender_id);
    out_host_header.seq_num = ntohl(net_hdr.seq_num);
    out_host_header.payload_len = ntohl(net_hdr.payload_len);

    if (PACKET_HEADER_SIZE + static_cast<size_t>(out_host_header.payload_len) > buffer_size) {
        return false;
    }

    return true;
}

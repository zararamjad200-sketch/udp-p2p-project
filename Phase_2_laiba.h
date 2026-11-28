#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <cstdint>
#include <string>
#include <vector>
#include <cstring>
#include <arpa/inet.h> // htonl, ntohl, htons, ntohs

constexpr uint8_t PROTOCOL_VERSION = 1;

enum class MessageType : uint8_t {  //enumeration named constants = group
    JOIN = 1,
    LEAVE = 2,
    HEARTBEAT = 3,
    CHAT = 4,
    ACK = 5,
    CHUNK_REQUEST = 6,
    CHUNK_DATA = 7
};

// Packed binary header (no compiler padding)
// All multi-byte fields are stored in Network Byte Order (Big-Endian)
#pragma pack(push, 1)
struct PacketHeader {
    uint8_t  version;      // Protocol version (1 byte)
    uint8_t  msg_type;     // MessageType enum value (1 byte)
    uint16_t flags;        // Reserved / control flags in network byte order (2 bytes)
    uint32_t sender_id;    // Sender peer ID in network byte order (4 bytes)
    uint32_t seq_num;      // Sequence number / chunk index in network byte order (4 bytes)
    uint32_t payload_len;  // Payload byte length in network byte order (4 bytes)
};
#pragma pack(pop)

constexpr size_t PACKET_HEADER_SIZE = sizeof(PacketHeader); // 16 bytes

// Builds a complete serialized byte buffer: [PacketHeader] + [Payload bytes]
std::vector<uint8_t> serialize_packet(
    MessageType type,
    uint32_t sender_id,
    uint32_t seq_num,
    const std::string& payload,
    uint16_t flags = 0
);

// Builds a serialized byte buffer from raw binary payload (for file chunks)
std::vector<uint8_t> serialize_binary_packet(
    MessageType type,
    uint32_t sender_id,
    uint32_t seq_num,
    const uint8_t* payload_data,
    uint32_t payload_len,
    uint16_t flags = 0
);

// Parses and validates header from incoming raw buffer (returns host byte order values)
bool deserialize_header(
    const uint8_t* buffer,
    size_t buffer_size,
    PacketHeader& out_host_header
);

#endif // PROTOCOL_H
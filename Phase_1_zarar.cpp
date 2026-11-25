#include "../include/socket_utils.h"
#include <cerrno>

bool set_non_blocking(int sockfd) {
    int flags = fcntl(sockfd, F_GETFL, 0);
    if (flags < 0) {
        return false;
    }
    return fcntl(sockfd, F_SETFL, flags | O_NONBLOCK) >= 0;
}

int create_and_bind_udp_socket(int port) {
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        return -1;
    }

    int opt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    int buf_bytes = 1048576; // 1 MB send/receive buffer for 64 KB chunks
    setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &buf_bytes, sizeof(buf_bytes));
    setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &buf_bytes, sizeof(buf_bytes));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(static_cast<uint16_t>(port));

    if (bind(sockfd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(sockfd);
        return -1;
    }

    return sockfd;
}

ssize_t send_udp_to(int sockfd, const std::string& ip, uint16_t port, const void* data, size_t len) {
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) <= 0) {
        return -1;
    }
    return sendto(sockfd, data, len, 0, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr));
}

ssize_t recv_udp_from(int sockfd, void* buf, size_t max_len, std::string& out_ip, uint16_t& out_port) {
    sockaddr_in src{};
    socklen_t addr_len = sizeof(src);
    ssize_t n = recvfrom(sockfd, buf, max_len, MSG_DONTWAIT, reinterpret_cast<sockaddr*>(&src), &addr_len);
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return 0;
        }
        return -1;
    }
    char ip_buf[INET_ADDRSTRLEN]{};
    if (inet_ntop(AF_INET, &src.sin_addr, ip_buf, sizeof(ip_buf)) != nullptr) {
        out_ip = ip_buf;
    }
    out_port = ntohs(src.sin_port);
    return n;
}
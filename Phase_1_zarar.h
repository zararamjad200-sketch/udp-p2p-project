#ifndef SOCKET_UTILS_H
#define SOCKET_UTILS_H

#include <iostream>
#include <string>
#include <cstring>
#include <cstdint>
#include <sys/types.h>
#include <fcntl.h>       // fcntl() and O_NONBLOCK for non-blocking sockets
#include <unistd.h>      // close() for closing socket file descriptors
#include <arpa/inet.h>   // htons(), htonl(), ntohs(), ntohl(), inet_pton(), inet_ntop()
#include <sys/socket.h>  // socket(), bind(), setsockopt(), sendto(), recvfrom()

// Sets a socket descriptor to non-blocking mode
bool set_non_blocking(int sockfd);

// Creates a UDP socket and binds it to the specified local port
int create_and_bind_udp_socket(int port);

// Sends a UDP datagram to ip:port
ssize_t send_udp_to(int sockfd, const std::string& ip, uint16_t port, const void* data, size_t len);

// Receives a non-blocking UDP datagram (returns 0 if no data is currently available)
ssize_t recv_udp_from(int sockfd, void* buf, size_t max_len, std::string& out_ip, uint16_t& out_port);

#endif // SOCKET_UTILS_H
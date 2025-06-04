#pragma once

#ifndef ASYNC_WRITER_H_HQIRH28I
#define ASYNC_WRITER_H_HQIRH28I

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <type_traits>
#include <vector>
#include <iostream> // For std::cout

#include "asio.hpp"
#include "rpc/config.h"
#include "rpc/msgpack.hpp"

#ifdef _WIN32
#include <windows.h>
#endif

namespace rpc {
namespace detail {

// Forward declaration helper function
template <typename SocketType>
struct socket_traits {
    static void close_socket(SocketType& socket, std::error_code& ec) {
        // Default implementation: call both shutdown and close
        socket.shutdown(RPCLIB_ASIO::socket_base::shutdown_both, ec);
        socket.close(ec);
    }
};

#ifdef _WIN32
// Windows platform stream_handle specialization
template <>
struct socket_traits<RPCLIB_ASIO::windows::stream_handle> {
    static void close_socket(RPCLIB_ASIO::windows::stream_handle& socket, std::error_code& ec) {
        // Windows stream_handle has no shutdown method, just call close
        socket.close(ec);
    }
};
// Synchronous HANDLE specialization (for Windows Named Pipe, sync mode)
template <>
struct socket_traits<HANDLE> {
    static void close_socket(HANDLE& handle, std::error_code& /*ec*/) {
        if (handle != INVALID_HANDLE_VALUE) {
            CloseHandle(handle);
            handle = INVALID_HANDLE_VALUE;
        }
    }
};
#endif

// Helper function for writing to different socket types
template <typename T>
inline void write_helper(T &s, RPCLIB_MSGPACK::sbuffer &buf, bool /* message_mode */ = false) {
    // Standard async_write for most socket types (TCP/Unix)
    RPCLIB_ASIO::async_write(
        s, RPCLIB_ASIO::buffer(buf.data(), buf.size()),
        [](std::error_code, std::size_t) {});
}

#ifdef _WIN32
// Synchronous write helper for Windows Named Pipe (HANDLE), replaces async_write/async_write_some
inline void write_helper(HANDLE hPipe, RPCLIB_MSGPACK::sbuffer &buf, bool message_mode = false) {
    if (message_mode) {
        uint32_t len = static_cast<uint32_t>(buf.size());
        DWORD written = 0;
        // Write framing header (4 bytes)
        if (!WriteFile(hPipe, &len, 4, &written, NULL) || written != 4) {
            std::cerr << "[sync_writer] Failed to write framing header, error: " << GetLastError() << std::endl;
            throw std::runtime_error("Failed to write framing header.");
        }
        // Write payload
        if (!WriteFile(hPipe, buf.data(), buf.size(), &written, NULL) || written != buf.size()) {
            std::cerr << "[sync_writer] Failed to write payload, error: " << GetLastError() << std::endl;
            throw std::runtime_error("Failed to write payload.");
        }
        std::cout << "[sync_writer] Wrote " << written << " bytes payload, total " << (4 + written) << " bytes (len=" << len << ")" << std::endl;
    } else {
        DWORD written = 0;
        if (!WriteFile(hPipe, buf.data(), buf.size(), &written, NULL) || written != buf.size()) {
            std::cerr << "[sync_writer] Failed to write raw buffer, error: " << GetLastError() << std::endl;
            throw std::runtime_error("Failed to write raw buffer.");
        }
        std::cout << "[sync_writer] Wrote raw buffer: " << written << " bytes" << std::endl;
    }
}
#endif

//! \brief Template class to support different socket types, with pipe framing logic for both write and read
template <typename SocketType>
class async_writer : public std::enable_shared_from_this<async_writer<SocketType>> {
public:
    async_writer(RPCLIB_ASIO::io_service *io, SocketType socket)
        : socket_(std::move(socket)),
          write_strand_(*io),
          is_writing_(false),
          exit_(false),
          is_closed_(false),
          message_mode_(false) {
#ifdef _WIN32
        // Enable message mode by default for Windows
        message_mode_ = true;
#endif
    }

    void write(RPCLIB_MSGPACK::sbuffer &&data);
    SocketType &socket();
    SocketType &stream() { return socket_; }
    RPCLIB_ASIO::strand &write_strand();
    void close();
    bool is_closed() const;
    void set_message_mode(bool enabled) { message_mode_ = enabled; }
    bool get_message_mode() const { return message_mode_; }

    // Static helper: read one framed message ([4 bytes length][data]), returns buffer with message
    // Usage: auto buf = async_writer<...>::read_framed(socket);
    static std::vector<uint8_t> read_framed(SocketType &sock) {
        uint32_t len = 0;
        // Read 4 bytes length prefix (blocking, can be replaced with async if needed)
        RPCLIB_ASIO::read(sock, RPCLIB_ASIO::buffer(&len, 4));
        if (len == 0) return {};
        std::vector<uint8_t> data(len);
        RPCLIB_ASIO::read(sock, RPCLIB_ASIO::buffer(data.data(), len));
        return data;
    }

protected:
    template <typename T>
    std::shared_ptr<T> shared_from_base() {
        return std::static_pointer_cast<T>(this->shared_from_this());
    }

    void write_handler(std::error_code ec, std::size_t transferred);

private:
    SocketType socket_;
    RPCLIB_ASIO::strand write_strand_;
    std::atomic_bool is_writing_;
    std::deque<RPCLIB_MSGPACK::sbuffer> write_queue_;
    std::atomic_bool exit_;
    std::atomic_bool is_closed_;
    bool message_mode_;
};

// Specialized as original type for backward compatibility
using async_writer_tcp = async_writer<RPCLIB_ASIO::ip::tcp::socket>;

// Template implementation part
template <typename SocketType>
void async_writer<SocketType>::write(RPCLIB_MSGPACK::sbuffer &&data) {
    std::cout << "[async_writer] write() called, message_mode_=" << message_mode_ << ", closed=" << is_closed_ << std::endl;
#ifdef _WIN32
    // If this is a Windows Named Pipe HANDLE, use synchronous WriteFile
    if (std::is_same<SocketType, HANDLE>::value && message_mode_) {
        write_helper(socket_, data, message_mode_);
        return;
    }
#endif
    // Original async queue logic for TCP/Unix
    write_queue_.push_back(std::move(data));
    if (!is_writing_ && !is_closed_) {
        is_writing_ = true;
        auto self(this->shared_from_this());
        write_strand_.post([this, self]() {
            if (exit_) {
                return;
            }
            auto &item = write_queue_.front();

            // For message_mode TCP/Unix, do framing here (not for Windows Pipe)
            if (message_mode_) {
                uint32_t len = static_cast<uint32_t>(item.size());
                std::vector<char> framed(4 + item.size());
                memcpy(framed.data(), &len, 4);
                memcpy(framed.data() + 4, item.data(), item.size());

                // --- Log framing header and part of content ---
                std::cout << "[async_writer] [framing write] Length prefix: " << len << ", total bytes: " << framed.size() << std::endl;
                std::cout << "[async_writer] [framing write] First 16 bytes (hex): ";
                for (size_t i = 0; i < std::min(size_t(16), framed.size()); ++i) {
                    printf("%02x ", (unsigned char)framed[i]);
                }
                std::cout << std::endl;

                RPCLIB_ASIO::async_write(
                    socket_, RPCLIB_ASIO::buffer(framed.data(), framed.size()),
                    write_strand_.wrap([this, self](std::error_code ec, std::size_t transferred) {
                        write_handler(ec, transferred);
                    })
                );
            } else {
                // Standard async write for non-message mode
                socket_.async_write_some(
                    RPCLIB_ASIO::buffer(item.data(), item.size()),
                    write_strand_.wrap(
                        [this, self](std::error_code ec, std::size_t transferred) {
                            write_handler(ec, transferred);
                        }));
            }
        });
    }
}

template <typename SocketType>
void async_writer<SocketType>::write_handler(std::error_code ec, std::size_t transferred) {
    if (exit_) {
        return;
    }

    if (!ec) {
        if (transferred < write_queue_.front().size()) {
            // sbuffer has no consume method, we need to create a new buffer
            RPCLIB_MSGPACK::sbuffer new_buf;
            const size_t remaining = write_queue_.front().size() - transferred;
            new_buf.write(write_queue_.front().data() + transferred, remaining);
            write_queue_.front() = std::move(new_buf);

            if (exit_) {
                return;
            }
            auto &item = write_queue_.front();
            auto self(this->shared_from_this());
            socket_.async_write_some(
                RPCLIB_ASIO::buffer(item.data(), item.size()),
                write_strand_.wrap([this, self](
                    std::error_code ec, std::size_t transferred) {
                    write_handler(ec, transferred);
                }));
        } else {
            write_queue_.pop_front();
            if (write_queue_.empty()) {
                is_writing_ = false;
            } else {
                if (exit_) {
                    return;
                }
                auto &item = write_queue_.front();
                auto self(this->shared_from_this());
                socket_.async_write_some(
                    RPCLIB_ASIO::buffer(item.data(), item.size()),
                    write_strand_.wrap(
                        [this, self](std::error_code ec,
                                  std::size_t transferred) {
                            write_handler(ec, transferred);
                        }));
            }
        }
    } else {
        is_writing_ = false;
        if (ec != RPCLIB_ASIO::error::operation_aborted) {
            // Client connection is broken. Remove this client from the server
            close();
        }
    }
}

template <typename SocketType>
SocketType &async_writer<SocketType>::socket() {
    return socket_;
}

template <typename SocketType>
RPCLIB_ASIO::strand &async_writer<SocketType>::write_strand() {
    return write_strand_;
}

template <typename SocketType>
void async_writer<SocketType>::close() {
    exit_ = true;
    is_closed_ = true;
    std::error_code ec;

    // Use specialized socket_traits to handle different socket types
    socket_traits<SocketType>::close_socket(socket_, ec);
}

template <typename SocketType>
bool async_writer<SocketType>::is_closed() const {
    return is_closed_;
}

#ifdef _WIN32
// Specialization for HANDLE (sync pipe)
template <>
class async_writer<HANDLE> : public std::enable_shared_from_this<async_writer<HANDLE>> {
public:
    async_writer(RPCLIB_ASIO::io_service * /*io*/, HANDLE handle)
        : handle_(handle), is_closed_(false), message_mode_(true) {}

    void write(RPCLIB_MSGPACK::sbuffer &&data) {
        ::rpc::detail::write_helper(handle_, data, message_mode_);
    }
    HANDLE &socket() { return handle_; }
    HANDLE &stream() { return handle_; }
    // Dummy strand (not used)
    RPCLIB_ASIO::strand &write_strand() { throw std::logic_error("Not implemented in sync HANDLE async_writer"); }
    void close() {
        if (!is_closed_) {
            std::error_code ec;
            socket_traits<HANDLE>::close_socket(handle_, ec);
            is_closed_ = true;
        }
    }
    bool is_closed() const { return is_closed_; }
    void set_message_mode(bool enabled) { message_mode_ = enabled; }
    bool get_message_mode() const { return message_mode_; }

    // Synchronous version: read one framed message ([4 bytes length][data]), returns buffer with message
    static std::vector<uint8_t> read_framed(HANDLE &hPipe) {
        uint32_t len = 0;
        DWORD read = 0;
        BOOL ok = ReadFile(hPipe, &len, 4, &read, NULL);
        if (!ok || read != 4) return {};
        if (len == 0) return {};
        std::vector<uint8_t> data(len);
        ok = ReadFile(hPipe, data.data(), len, &read, NULL);
        if (!ok || read != len) return {};
        return data;
    }

protected:
    template <typename T>
    std::shared_ptr<T> shared_from_base() {
        return std::static_pointer_cast<T>(this->shared_from_this());
    }
private:
    HANDLE handle_;
    bool is_closed_;
    bool message_mode_;
};
#endif

} /* detail */
} /* rpc */

#endif /* end of include guard: ASYNC_WRITER_H_HQIRH28I */
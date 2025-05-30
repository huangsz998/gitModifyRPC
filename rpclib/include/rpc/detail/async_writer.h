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

#include "asio.hpp"

#include "rpc/config.h"
#include "rpc/msgpack.hpp"

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

// Windows platform stream_handle specialization
#ifdef _WIN32
template <>
struct socket_traits<RPCLIB_ASIO::windows::stream_handle> {
    static void close_socket(RPCLIB_ASIO::windows::stream_handle& socket, std::error_code& ec) {
        // Windows stream_handle has no shutdown method, just call close
        socket.close(ec);
    }
};
#endif

// Helper function for writing to different socket types - FIXED WITH COMMON INTERFACE
template <typename T>
inline void write_helper(T &s, RPCLIB_MSGPACK::sbuffer &buf, bool /* message_mode */ = false) {
    // Standard async_write for most socket types
    RPCLIB_ASIO::async_write(
        s, RPCLIB_ASIO::buffer(buf.data(), buf.size()),
        [](std::error_code, std::size_t) {});
}

#ifdef _WIN32
// Specialized write helper for Windows Named Pipes with message mode support
template <>
inline void write_helper<RPCLIB_ASIO::windows::stream_handle>(
    RPCLIB_ASIO::windows::stream_handle &s, 
    RPCLIB_MSGPACK::sbuffer &buf, 
    bool message_mode) {
        
    if (message_mode) {
        // For message mode, use synchronous writes to ensure complete message
        try {
            RPCLIB_ASIO::write(
                s, RPCLIB_ASIO::buffer(buf.data(), buf.size()));
        }
        catch (const std::exception&) {
            // Handle write errors - log if needed
            // Note: Using parameter variable so compiler won't warn
        }
    } else {
        // Standard byte-oriented async write
        RPCLIB_ASIO::async_write(
            s, RPCLIB_ASIO::buffer(buf.data(), buf.size()),
            [](std::error_code, std::size_t) {});
    }
}
#endif

//! \brief Template class to support different socket types
template <typename SocketType>
class async_writer : public std::enable_shared_from_this<async_writer<SocketType>> {
public:
    async_writer(RPCLIB_ASIO::io_service *io, SocketType socket)
        : socket_(std::move(socket)),
          write_strand_(*io),
          is_writing_(false),
          exit_(false),
          is_closed_(false),
          message_mode_(false) {} // Added message_mode_ flag initialization

    void write(RPCLIB_MSGPACK::sbuffer &&data);
    
    SocketType &socket();
    
    RPCLIB_ASIO::strand &write_strand();
    
    void close();
    
    bool is_closed() const;
    
    // Add setter for message mode
    void set_message_mode(bool enabled) {
        message_mode_ = enabled;
    }
    
    // Add getter for message mode
    bool get_message_mode() const {
        return message_mode_;
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
    bool message_mode_; // Flag to indicate if using message mode
};

// Specialized as original type for backward compatibility
using async_writer_tcp = async_writer<RPCLIB_ASIO::ip::tcp::socket>;

// Template implementation part
template <typename SocketType>
void async_writer<SocketType>::write(RPCLIB_MSGPACK::sbuffer &&data) {
    write_queue_.push_back(std::move(data));
    if (!is_writing_ && !is_closed_) {
        is_writing_ = true;
        auto self(this->shared_from_this());
        write_strand_.post([this, self]() {
            if (exit_) {
                return;
            }
            auto &item = write_queue_.front();
            
#ifdef _WIN32
            // Check if this is a Windows named pipe with message mode
            if (std::is_same<SocketType, RPCLIB_ASIO::windows::stream_handle>::value && message_mode_) {
                // Use specialized helper for Windows Named Pipes
                write_helper(socket_, item, message_mode_);
                
                // Handle completion directly for message mode
                write_queue_.pop_front();
                is_writing_ = false;
                
                // Process next item in queue if any
                if (!write_queue_.empty() && !exit_) {
                    write(std::move(RPCLIB_MSGPACK::sbuffer())); // Dummy to trigger processing
                }
                return;
            }
#endif
            // Standard async write for non-Windows or non-message mode
            socket_.async_write_some(
                RPCLIB_ASIO::buffer(item.data(), item.size()),
                write_strand_.wrap(
                    [this, self](std::error_code ec, std::size_t transferred) {
                        write_handler(ec, transferred);
                    }));
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

} /* detail */
} /* rpc */

#endif /* end of include guard: ASYNC_WRITER_H_HQIRH28I */
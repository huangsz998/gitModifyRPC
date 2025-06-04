#pragma once

#ifndef SERVER_PIPE_SESSION_H
#define SERVER_PIPE_SESSION_H

#include "asio.hpp"
#include <memory>
#include <vector>
#include <iostream>

#include "rpc/config.h"
#include "rpc/msgpack.hpp"

#include "rpc/dispatcher.h"
#include "rpc/detail/async_writer.h"
#include "rpc/detail/log.h"

namespace rpc {

class server;
class this_handler_t;
class this_session_t;
class this_server_t;

namespace detail {

// Session class specialized for Named Pipes or sockets
template<typename SocketType>
class server_pipe_session : public async_writer<SocketType> {
public:
    server_pipe_session(server *srv, RPCLIB_ASIO::io_service *io,
                   SocketType socket,
                   std::shared_ptr<dispatcher> disp, bool suppress_exceptions);

    void start();
    void close();

    // Set message mode flag for Windows Named Pipes and propagate to async_writer
    void set_message_mode(bool enabled) {
        message_mode_ = enabled;
        std::cout << "[server_pipe_session::set_message_mode] set to " << enabled << std::endl;
        async_writer<SocketType>::set_message_mode(enabled); // propagate to base
    }

private:
    void do_read();
    void setup_read_buf();
    void process_message(RPCLIB_MSGPACK::object_handle& msg_obj);
    void process_raw_message(const char* data, size_t size);
    void do_read_message_mode();

private:
    friend class rpc::this_handler_t;
    friend class rpc::this_session_t;
    friend class rpc::this_server_t;

    server* parent_;
    RPCLIB_ASIO::io_service *io_;
    RPCLIB_ASIO::strand read_strand_;
    std::shared_ptr<dispatcher> disp_;
    RPCLIB_MSGPACK::unpacker pac_;
    RPCLIB_MSGPACK::sbuffer output_buf_;
    std::vector<char> read_buffer_; // Buffer for message reading
    bool message_mode_; // Flag for message mode
    const bool suppress_exceptions_;
#ifdef _WIN32
    std::shared_ptr<std::vector<char>> framing_buffer_; // Sticky packet buffer for Windows
#endif
    RPCLIB_CREATE_LOG_CHANNEL(pipe_session)
};

} /* detail */
} /* rpc */

// Template implementation
#include "rpc/detail/server_pipe_session.inl"
#endif /* end of include guard: SERVER_PIPE_SESSION_H */
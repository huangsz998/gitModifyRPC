#pragma once

#include "rpc/detail/server_pipe_session.h"
#include "rpc/config.h"
#include "rpc/server.h"
#include "rpc/this_handler.h"
#include "rpc/this_server.h"
#include "rpc/this_session.h"
#include "rpc/detail/log.h"

#ifdef _WIN32
#include <windows.h>
#endif

namespace rpc {

extern bool enable_pipe_debug;
extern bool enable_message_tracking;

namespace detail {

static constexpr std::size_t pipe_default_buffer_size =
    rpc::constants::DEFAULT_BUFFER_SIZE;
static constexpr std::size_t pipe_message_buffer_size = 65536;

template<typename SocketType>
server_pipe_session<SocketType>::server_pipe_session(
    server *srv, RPCLIB_ASIO::io_service *io,
    SocketType socket, std::shared_ptr<dispatcher> disp,
    bool suppress_exceptions)
    : async_writer<SocketType>(io, std::move(socket)),
      parent_(srv),
      io_(io),
      read_strand_(*io),
      disp_(disp),
      pac_(),
      message_mode_(false),
      suppress_exceptions_(suppress_exceptions)
#ifdef _WIN32
      ,framing_buffer_(nullptr)
#endif
{
    LOG_INFO("server_pipe_session: constructor called");
    std::cout << "[server_pipe_session::ctor] constructed" << std::endl;

    pac_.reserve_buffer(pipe_default_buffer_size);
    read_buffer_.resize(pipe_message_buffer_size);

#ifdef _WIN32
    // Windows named pipe always uses message mode
    message_mode_ = true;
    async_writer<SocketType>::set_message_mode(true);
    framing_buffer_ = std::make_shared<std::vector<char>>();
    LOG_INFO("server_pipe_session: [Windows] message_mode_ set to true by default");
    std::cout << "[server_pipe_session::ctor] [Windows] message_mode_ = true" << std::endl;
#endif
}

template<typename SocketType>
void server_pipe_session<SocketType>::start() {
    LOG_INFO("server_pipe_session: start() called");
    std::cout << "[server_pipe_session::start] called, message_mode_=" << message_mode_ << std::endl;
    setup_read_buf();

    if (message_mode_) {
        LOG_INFO("server_pipe_session: Using message mode for pipe session");
        std::cout << "[server_pipe_session::start] entering do_read_message_mode()" << std::endl;
        do_read_message_mode();
    } else {
        LOG_INFO("server_pipe_session: Using standard byte mode for pipe session");
        std::cout << "[server_pipe_session::start] entering do_read()" << std::endl;
        do_read();
    }
}

template<typename SocketType>
void server_pipe_session<SocketType>::setup_read_buf() {
    LOG_INFO("server_pipe_session: Setting up read buffer");
    std::cout << "[server_pipe_session::setup_read_buf] zeroing read_buffer_, size=" << read_buffer_.size() << std::endl;
    std::fill(read_buffer_.begin(), read_buffer_.end(), 0);
}

template<typename SocketType>
void server_pipe_session<SocketType>::close() {
    LOG_INFO("server_pipe_session: Closing pipe session");
    std::cout << "[server_pipe_session::close] called" << std::endl;
    async_writer<SocketType>::close();

    auto self = std::static_pointer_cast<server_pipe_session<SocketType>>(
        async_writer<SocketType>::shared_from_this());

    this->write_strand().post([this, self]() {
        std::cout << "[server_pipe_session::close] invoking parent_->close_pipe_session()" << std::endl;
        parent_->close_pipe_session(self);
    });
}

template<typename SocketType>
void server_pipe_session<SocketType>::process_raw_message(const char* data, size_t size) {
    LOG_INFO("server_pipe_session: process_raw_message() called, size={}", size);
    std::cout << "[server_pipe_session::process_raw_message] called, size=" << size << std::endl;

    try {
        if (enable_pipe_debug || enable_message_tracking) {
            std::string hex_dump;
            for (size_t i = 0; i < std::min(size, size_t(64)); ++i) {
                char buf[8];
                snprintf(buf, sizeof(buf), "%02x ", (unsigned char)data[i]);
                hex_dump += buf;
            }
            LOG_INFO("Raw message content (first 64 bytes): {}", hex_dump);
            std::cout << "[server_pipe_session::process_raw_message] hex dump: " << hex_dump << std::endl;
        }

        RPCLIB_MSGPACK::unpacked result;
        RPCLIB_MSGPACK::unpack(result, data, size);
        std::cout << "[server_pipe_session::process_raw_message] unpacked msg, calling process_message()" << std::endl;
        process_message(result);
    }
    catch (const std::exception& e) {
        LOG_ERROR("Error processing raw message: {}", e.what());
        std::cerr << "Raw message processing error: " << e.what() << std::endl;
    }
}

template<typename SocketType>
void server_pipe_session<SocketType>::process_message(RPCLIB_MSGPACK::object_handle& result) {
    LOG_INFO("server_pipe_session: process_message() called");
    std::cout << "[server_pipe_session::process_message] called" << std::endl;

    auto self = std::static_pointer_cast<server_pipe_session<SocketType>>(
        async_writer<SocketType>::shared_from_this());

    auto msg = result.get();
    output_buf_.clear();

    try {
        rpc::this_handler().clear();
        rpc::this_session().clear();
        rpc::this_session().set_id(reinterpret_cast<rpc::session_id_t>(this));
        rpc::this_server().cancel_stop();

        uint32_t call_id = 0;
        try {
            if (msg.type == RPCLIB_MSGPACK::type::ARRAY && msg.via.array.size >= 4) {
                call_id = msg.via.array.ptr[1].as<uint32_t>();
                LOG_INFO("Processing RPC call ID: {}", call_id);
                std::string method = msg.via.array.ptr[2].as<std::string>();
                LOG_INFO("RPC method: '{}', args count: {}", method,
                    msg.via.array.size > 3 ? msg.via.array.ptr[3].via.array.size : 0);
                std::cout << "[server_pipe_session::process_message] call_id: " << call_id
                    << ", method: " << method << std::endl;
            }
        } catch (...) {
            LOG_WARN("Could not extract call details from message");
            std::cout << "[server_pipe_session::process_message] Could not extract call details" << std::endl;
        }

        LOG_INFO("Processing request");
        std::cout << "[server_pipe_session::process_message] calling dispatcher->dispatch()" << std::endl;
        auto resp = disp_->dispatch(msg, suppress_exceptions_);

        if (!rpc::this_handler().resp_enabled_) {
            LOG_INFO("Response disabled, not sending anything back");
            std::cout << "[server_pipe_session::process_message] response disabled" << std::endl;
            return;
        }

        if (!rpc::this_handler().error_.get().is_nil()) {
            LOG_WARN("Handler reported error");
            std::cout << "[server_pipe_session::process_message] handler reported error" << std::endl;
            resp.capture_error(rpc::this_handler().error_);
        } else if (!rpc::this_handler().resp_.get().is_nil()) {
            LOG_INFO("Handler provided special response");
            std::cout << "[server_pipe_session::process_message] handler provided special response" << std::endl;
            resp.capture_result(rpc::this_handler().resp_);
        }

        if (!resp.is_empty()) {
            LOG_INFO("Sending response for call ID: {}", call_id);
            std::cout << "[server_pipe_session::process_message] sending response for call_id: " << call_id << std::endl;
            this->write(resp.get_data());
            LOG_INFO("server_pipe_session: write() called after process_message");
            std::cout << "[server_pipe_session::process_message] write() called" << std::endl;
        }

        if (rpc::this_session().exit_) {
            LOG_WARN("Session exit requested from handler");
            std::cout << "[server_pipe_session::process_message] session exit requested" << std::endl;
            this->close();
            return;
        }

        if (rpc::this_server().stopping()) {
            LOG_WARN("Server exit requested from handler");
            std::cout << "[server_pipe_session::process_message] server exit requested" << std::endl;
            parent_->close_sessions();
            return;
        }
    }
    catch (const std::exception& e) {
        LOG_ERROR("Exception during message processing: {}", e.what());
        std::cerr << "Processing error: " << e.what() << std::endl;
    }
}

#ifdef _WIN32
// Synchronous message mode read for Windows Named Pipe (HANDLE)
template<>
void server_pipe_session<HANDLE>::do_read_message_mode() {
    LOG_INFO("[server_pipe_session::do_read_message_mode] (SYNC HANDLE) start");
    std::cout << "[server_pipe_session::do_read_message_mode] [SYNC HANDLE] entering blocking read loop" << std::endl;

    while (!this->is_closed()) {
        uint32_t msglen = 0;
        DWORD read = 0;

        // Step 1: Read 4-byte length header
        BOOL ok = ReadFile(this->socket(), &msglen, 4, &read, NULL);
        if (!ok || read != 4) {
            DWORD err = GetLastError();
            std::cerr << "[server_pipe_session::do_read_message_mode] ReadFile (length) failed, err=" << err << std::endl;
            LOG_ERROR("ReadFile (length) failed, err={}", err);
            this->close();
            break;
        }
        std::cout << "[server_pipe_session::do_read_message_mode] Got frame length: " << msglen << std::endl;

        if (msglen == 0 || msglen > (16 * 1024 * 1024)) {
            std::cerr << "[server_pipe_session::do_read_message_mode] Invalid message size: " << msglen << std::endl;
            LOG_ERROR("Invalid message size: {}", msglen);
            this->close();
            break;
        }

        // Step 2: Read payload
        std::vector<char> payload(msglen);
        ok = ReadFile(this->socket(), payload.data(), msglen, &read, NULL);
        if (!ok || read != msglen) {
            DWORD err = GetLastError();
            std::cerr << "[server_pipe_session::do_read_message_mode] ReadFile (payload) failed, err=" << err << std::endl;
            LOG_ERROR("ReadFile (payload) failed, err={}", err);
            this->close();
            break;
        }
        std::cout << "[server_pipe_session::do_read_message_mode] Processing complete frame of size: " << msglen << std::endl;
        process_raw_message(payload.data(), msglen);
    }
}
#endif

#ifndef _WIN32
template<typename SocketType>
void server_pipe_session<SocketType>::do_read_message_mode() {
    LOG_INFO("Starting message-mode read operation");
    std::cout << "[server_pipe_session::do_read_message_mode] called (unix)" << std::endl;

    auto self = std::static_pointer_cast<server_pipe_session<SocketType>>(
        async_writer<SocketType>::shared_from_this());

    this->socket().async_read_some(
        RPCLIB_ASIO::buffer(read_buffer_.data(), read_buffer_.size()),
        read_strand_.wrap([this, self](std::error_code ec, std::size_t bytes_read) {
            if (this->is_closed()) {
                LOG_INFO("Session closed, stopping read");
                std::cout << "[server_pipe_session::do_read_message_mode] session closed" << std::endl;
                return;
            }

            if (!ec) {
                if (bytes_read == 0) {
                    LOG_WARN("Read zero bytes in message mode, retrying");
                    std::cout << "[server_pipe_session::do_read_message_mode] read zero bytes" << std::endl;
                    do_read_message_mode();
                    return;
                }

                LOG_INFO("Read {} bytes in message mode", bytes_read);
                std::cout << "[server_pipe_session::do_read_message_mode] read " << bytes_read << " bytes" << std::endl;

                try {
                    process_raw_message(read_buffer_.data(), bytes_read);

                    if (!this->is_closed()) {
                        std::fill(read_buffer_.begin(), read_buffer_.end(), 0);
                        do_read_message_mode();
                    }
                }
                catch (const std::exception& e) {
                    LOG_ERROR("Exception during message mode read: {}", e.what());
                    std::cerr << "Message mode read error: " << e.what() << std::endl;
                    if (!this->is_closed()) {
                        do_read_message_mode();
                    }
                }
            } else if (ec == RPCLIB_ASIO::error::eof ||
                      ec == RPCLIB_ASIO::error::connection_reset) {
                LOG_INFO("Client disconnected");
                std::cout << "[server_pipe_session::do_read_message_mode] client disconnected" << std::endl;
                self->close();
            } else {
                LOG_ERROR("Error reading in message mode: {} | '{}'", ec, ec.message());
                std::cout << "[server_pipe_session::do_read_message_mode] error: " << ec.message() << std::endl;
                self->close();
            }
        }));
}
#endif

#ifdef _WIN32
// Synchronous (blocking) do_read for Windows Named Pipe (HANDLE)
template<>
void server_pipe_session<HANDLE>::do_read() {
    do_read_message_mode(); // always use message mode
}
#endif

#ifndef _WIN32
template<typename SocketType>
void server_pipe_session<SocketType>::do_read() {
    if (message_mode_) {
        do_read_message_mode();
        return;
    }

    LOG_INFO("server_pipe_session: do_read() called");
    std::cout << "[server_pipe_session::do_read] called (unix)" << std::endl;

    auto self = std::static_pointer_cast<server_pipe_session<SocketType>>(
        async_writer<SocketType>::shared_from_this());

    constexpr std::size_t max_read_bytes = pipe_default_buffer_size;
    this->socket().async_read_some(
        RPCLIB_ASIO::buffer(pac_.buffer(), pipe_default_buffer_size),
        read_strand_.wrap([this, self, max_read_bytes](std::error_code ec,
                                                     std::size_t length) {
            if (this->is_closed()) {
                std::cout << "[server_pipe_session::do_read] session closed" << std::endl;
                return;
            }
            if (!ec) {
                if (length == 0) {
                    LOG_WARN("Read zero bytes, retrying");
                    std::cout << "[server_pipe_session::do_read] read zero bytes" << std::endl;
                    do_read();
                    return;
                }

                pac_.buffer_consumed(length);
                RPCLIB_MSGPACK::object_handle result;
                while (pac_.next(result) && !this->is_closed()) {
                    process_message(result);
                }

                if (!this->is_closed()) {
                    if (pac_.buffer_capacity() < max_read_bytes) {
                        LOG_TRACE("Reserving extra buffer: {}", max_read_bytes);
                        pac_.reserve_buffer(max_read_bytes);
                    }
                    do_read();
                }
            } else if (ec == RPCLIB_ASIO::error::eof ||
                       ec == RPCLIB_ASIO::error::connection_reset) {
                LOG_INFO("Client disconnected");
                std::cout << "[server_pipe_session::do_read] client disconnected" << std::endl;
                self->close();
            } else {
                LOG_ERROR("Unhandled error code: {} | '{}'", ec, ec.message());
                std::cout << "[server_pipe_session::do_read] error: " << ec.message() << std::endl;
            }
        }));
}
#endif

} /* detail */
} /* rpc */
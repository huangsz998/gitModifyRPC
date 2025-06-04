#pragma once

#include "rpc/detail/server_pipe_session.h"

#include "rpc/config.h"
#include "rpc/server.h"
#include "rpc/this_handler.h"
#include "rpc/this_server.h"
#include "rpc/this_session.h"

#include "rpc/detail/log.h"

namespace rpc {

// External declaration for pipe debugging flag
extern bool enable_pipe_debug;
extern bool enable_message_tracking;

namespace detail {

static constexpr std::size_t pipe_default_buffer_size =
    rpc::constants::DEFAULT_BUFFER_SIZE;

// Larger buffer size for message mode
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
      suppress_exceptions_(suppress_exceptions) {
    LOG_INFO("server_pipe_session: constructor called");
    pac_.reserve_buffer(pipe_default_buffer_size);
    read_buffer_.resize(pipe_message_buffer_size);
}

template<typename SocketType>
void server_pipe_session<SocketType>::start() {
    LOG_INFO("server_pipe_session: start() called");
    setup_read_buf();

    if (message_mode_) {
        LOG_INFO("server_pipe_session: Using message mode for pipe session");
        do_read_message_mode();
    } else {
        LOG_INFO("server_pipe_session: Using standard byte mode for pipe session");
        do_read();
    }
}

template<typename SocketType>
void server_pipe_session<SocketType>::setup_read_buf() {
    LOG_INFO("server_pipe_session: Setting up read buffer for pipe session");
    std::fill(read_buffer_.begin(), read_buffer_.end(), 0);
}

template<typename SocketType>
void server_pipe_session<SocketType>::close() {
    LOG_INFO("server_pipe_session: Closing pipe session.");
    async_writer<SocketType>::close();

    auto self = std::static_pointer_cast<server_pipe_session<SocketType>>(
        async_writer<SocketType>::shared_from_this());

    this->write_strand().post([this, self]() {
        parent_->close_pipe_session(self);
    });
}

template<typename SocketType>
void server_pipe_session<SocketType>::process_raw_message(const char* data, size_t size) {
    LOG_INFO("server_pipe_session: process_raw_message() called, size={}", size);
    try {
        if (enable_pipe_debug || enable_message_tracking) {
            std::string hex_dump;
            for (size_t i = 0; i < std::min(size, size_t(64)); ++i) {
                char buf[8];
                snprintf(buf, sizeof(buf), "%02x ", (unsigned char)data[i]);
                hex_dump += buf;
            }
            LOG_INFO("Raw message content (first 64 bytes): {}", hex_dump);
        }

        RPCLIB_MSGPACK::unpacked result;
        RPCLIB_MSGPACK::unpack(result, data, size);
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
            }
        } catch (...) {
            LOG_WARN("Could not extract call details from message");
        }

        LOG_INFO("Processing request");
        auto resp = disp_->dispatch(msg, suppress_exceptions_);

        if (!rpc::this_handler().resp_enabled_) {
            LOG_INFO("Response disabled, not sending anything back");
            return;
        }

        if (!rpc::this_handler().error_.get().is_nil()) {
            LOG_WARN("Handler reported error");
            resp.capture_error(rpc::this_handler().error_);
        } else if (!rpc::this_handler().resp_.get().is_nil()) {
            LOG_INFO("Handler provided special response");
            resp.capture_result(rpc::this_handler().resp_);
        }

        if (!resp.is_empty()) {
            LOG_INFO("Sending response for call ID: {}", call_id);
            this->write(resp.get_data());
            LOG_INFO("server_pipe_session: write() called after process_message");
        }

        if (rpc::this_session().exit_) {
            LOG_WARN("Session exit requested from a handler.");
            this->close();
            return;
        }

        if (rpc::this_server().stopping()) {
            LOG_WARN("Server exit requested from a handler.");
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
template<>
void server_pipe_session<RPCLIB_ASIO::windows::stream_handle>::do_read_message_mode() {
    LOG_INFO("server_pipe_session: do_read_message_mode() called");

    auto self = std::static_pointer_cast<server_pipe_session<RPCLIB_ASIO::windows::stream_handle>>(
        async_writer<RPCLIB_ASIO::windows::stream_handle>::shared_from_this());

    auto read_length = std::make_shared<std::vector<char>>(4);
    RPCLIB_ASIO::async_read(
        this->socket(),
        RPCLIB_ASIO::buffer(*read_length),
        read_strand_.wrap([this, self, read_length](std::error_code ec, std::size_t bytes_transferred) {
            LOG_INFO("server_pipe_session: async_read (length) callback called");
            if (this->is_closed()) return;
            if (ec || bytes_transferred != 4) {
                LOG_ERROR("Error reading message length ({} bytes): {} | '{}'", bytes_transferred, ec.value(), ec.message());
                this->close();
                return;
            }
            uint32_t msglen = 0;
            memcpy(&msglen, read_length->data(), 4);
            LOG_INFO("server_pipe_session: Message length: {}", msglen);

            if (msglen == 0 || msglen > (16*1024*1024)) {
                LOG_ERROR("Invalid message length: {}", msglen);
                this->close();
                return;
            }
            auto payload = std::make_shared<std::vector<char>>(msglen);
            RPCLIB_ASIO::async_read(
                this->socket(),
                RPCLIB_ASIO::buffer(*payload),
                read_strand_.wrap([this, self, payload, msglen](std::error_code ec2, std::size_t bytes_transferred2) {
                    LOG_INFO("server_pipe_session: async_read (payload) callback called");
                    if (this->is_closed()) return;
                    if (ec2 || bytes_transferred2 != msglen) {
                        LOG_ERROR("Error reading payload ({} bytes): {} | '{}'", bytes_transferred2, ec2.value(), ec2.message());
                        this->close();
                        return;
                    }
                    LOG_INFO("server_pipe_session: Received framed message, {} bytes", msglen);
                    process_raw_message(reinterpret_cast<const char*>(payload->data()), msglen);
                    if (!this->is_closed())
                        do_read_message_mode();
                })
            );
        })
    );
}
#endif

#ifndef _WIN32
template<typename SocketType>
void server_pipe_session<SocketType>::do_read_message_mode() {
    LOG_INFO("Starting message-mode read operation");

    auto self = std::static_pointer_cast<server_pipe_session<SocketType>>(
        async_writer<SocketType>::shared_from_this());

    this->socket().async_read_some(
        RPCLIB_ASIO::buffer(read_buffer_.data(), read_buffer_.size()),
        read_strand_.wrap([this, self](std::error_code ec, std::size_t bytes_read) {
            if (this->is_closed()) {
                LOG_INFO("Session closed, stopping read");
                return;
            }

            if (!ec) {
                if (bytes_read == 0) {
                    LOG_WARN("Read zero bytes in message mode, retrying");
                    do_read_message_mode();
                    return;
                }

                LOG_INFO("Read {} bytes in message mode", bytes_read);

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
                self->close();
            } else {
                LOG_ERROR("Error reading in message mode: {} | '{}'", ec, ec.message());
                self->close();
            }
        }));
}
#endif

#ifdef _WIN32
template<>
void server_pipe_session<RPCLIB_ASIO::windows::stream_handle>::do_read() {
    LOG_INFO("server_pipe_session: do_read() called");
    if (message_mode_) {
        do_read_message_mode();
        return;
    }

    LOG_INFO("Starting read operation on Windows Named Pipe (byte mode)");

    auto self = std::static_pointer_cast<server_pipe_session>(
        async_writer<RPCLIB_ASIO::windows::stream_handle>::shared_from_this());

    this->socket().async_read_some(
        RPCLIB_ASIO::buffer(read_buffer_.data(), read_buffer_.size()),
        read_strand_.wrap([this, self](std::error_code ec, std::size_t length) {
            if (this->is_closed()) {
                LOG_INFO("Pipe session closed, stopping read");
                return;
            }

            if (!ec) {
                if (length == 0) {
                    LOG_WARN("Read zero bytes from Windows Named Pipe, retrying");
                    do_read();
                    return;
                }

                LOG_INFO("Read {} bytes from Windows Named Pipe", length);

                try {
                    if (enable_pipe_debug || enable_message_tracking) {
                        std::string hex_dump;
                        for (size_t i = 0; i < std::min(length, size_t(64)); ++i) {
                            char buf[8];
                            snprintf(buf, sizeof(buf), "%02x ", (unsigned char)read_buffer_[i]);
                            hex_dump += buf;
                        }
                        LOG_INFO("Received data (first 64 bytes): {}", hex_dump);
                    }

                    pac_.reserve_buffer(length);
                    memcpy(pac_.buffer(), read_buffer_.data(), length);
                    pac_.buffer_consumed(length);

                    RPCLIB_MSGPACK::object_handle result;
                    while (pac_.next(result) && !this->is_closed()) {
                        LOG_INFO("Processing message");
                        process_message(result);
                    }

                    if (!this->is_closed()) {
                        if (pac_.buffer_capacity() < pipe_default_buffer_size) {
                            LOG_INFO("Reserving more buffer: {}", pipe_default_buffer_size);
                            pac_.reserve_buffer(pipe_default_buffer_size);
                        }
                        std::fill(read_buffer_.begin(), read_buffer_.end(), 0);
                        do_read();
                    }
                }
                catch (const std::exception& e) {
                    LOG_ERROR("Exception during Windows Named Pipe read processing: {}", e.what());
                    std::cerr << "Exception in pipe read: " << e.what() << std::endl;
                    if (!this->is_closed()) {
                        do_read();
                    }
                }
            } else if (ec == RPCLIB_ASIO::error::eof ||
                       ec == RPCLIB_ASIO::error::connection_reset) {
                LOG_INFO("Windows Named Pipe client disconnected");
                self->close();
            } else {
                LOG_ERROR("Windows Named Pipe error: {} | '{}'", ec, ec.message());
                self->close();
            }
        }));
}
#else
template<typename SocketType>
void server_pipe_session<SocketType>::do_read() {
    LOG_INFO("server_pipe_session: do_read() called");
    if (message_mode_) {
        do_read_message_mode();
        return;
    }

    auto self = std::static_pointer_cast<server_pipe_session<SocketType>>(
        async_writer<SocketType>::shared_from_this());

    constexpr std::size_t max_read_bytes = pipe_default_buffer_size;
    this->socket().async_read_some(
        RPCLIB_ASIO::buffer(pac_.buffer(), pipe_default_buffer_size),
        read_strand_.wrap([this, self, max_read_bytes](std::error_code ec,
                                                     std::size_t length) {
            if (this->is_closed()) { return; }
            if (!ec) {
                if (length == 0) {
                    LOG_WARN("Read zero bytes, retrying");
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
                self->close();
            } else {
                LOG_ERROR("Unhandled error code: {} | '{}'", ec, ec.message());
            }
        }));
}
#endif

} /* detail */
} /* rpc */
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
    pac_.reserve_buffer(pipe_default_buffer_size);
    read_buffer_.resize(pipe_message_buffer_size); // Use larger buffer for message mode
}

template<typename SocketType>
void server_pipe_session<SocketType>::start() {
    LOG_INFO("Starting pipe session");
    setup_read_buf();
    
    // Choose reading method based on message mode flag
    if (message_mode_) {
        LOG_INFO("Using message mode for pipe session");
        do_read_message_mode();
    } else {
        LOG_INFO("Using standard byte mode for pipe session");
        do_read();
    }
}

template<typename SocketType>
void server_pipe_session<SocketType>::setup_read_buf() {
    // Default implementation for all socket types
    LOG_INFO("Setting up read buffer for pipe session");
    std::fill(read_buffer_.begin(), read_buffer_.end(), 0);
}

#ifdef _WIN32
// Windows-specific implementation for stream_handle
template<>
void server_pipe_session<RPCLIB_ASIO::windows::stream_handle>::setup_read_buf() {
    LOG_INFO("Setting up read buffer for Windows Named Pipe");
    std::fill(read_buffer_.begin(), read_buffer_.end(), 0);
    // Additional Windows-specific setup can be added here
}
#endif

template<typename SocketType>
void server_pipe_session<SocketType>::close() {
    LOG_INFO("Closing pipe session.");
    async_writer<SocketType>::close();

    // Use class name to explicitly specify shared_from_this
    auto self = std::static_pointer_cast<server_pipe_session<SocketType>>(
        async_writer<SocketType>::shared_from_this());
        
    this->write_strand().post([this, self]() {
        parent_->close_pipe_session(self);
    });
}

// Special method for processing raw message data
template<typename SocketType>
void server_pipe_session<SocketType>::process_raw_message(const char* data, size_t size) {
    try {
        // Debug logging of message content
        if (enable_pipe_debug || enable_message_tracking) {
            std::string hex_dump;
            for (size_t i = 0; i < std::min(size, size_t(64)); ++i) { // Increased dump size to 64 bytes
                char buf[8];
                snprintf(buf, sizeof(buf), "%02x ", (unsigned char)data[i]);
                hex_dump += buf;
            }
            LOG_INFO("Raw message content (first 64 bytes): {}", hex_dump);
        }
        
        // Process message data with msgpack
        RPCLIB_MSGPACK::unpacked result;
        RPCLIB_MSGPACK::unpack(result, data, size);
        
        // Process the unpacked message
        process_message(result);
    }
    catch (const std::exception& e) {
        LOG_ERROR("Error processing raw message: {}", e.what());
        std::cerr << "Raw message processing error: " << e.what() << std::endl;
    }
}

template<typename SocketType>
void server_pipe_session<SocketType>::process_message(RPCLIB_MSGPACK::object_handle& result) {
    // Explicitly specify which base class to call shared_from_this from
    auto self = std::static_pointer_cast<server_pipe_session<SocketType>>(
        async_writer<SocketType>::shared_from_this());
        
    auto msg = result.get();
    output_buf_.clear();

    // DIRECT PROCESSING for Windows Named Pipe in message mode
    if (message_mode_) {
        try {
            // Setup context
            rpc::this_handler().clear();
            rpc::this_session().clear();
            rpc::this_session().set_id(reinterpret_cast<rpc::session_id_t>(this));
            rpc::this_server().cancel_stop();

            // Extract and record message ID and method for debugging
            uint32_t call_id = 0;
            try {
                if (msg.type == RPCLIB_MSGPACK::type::ARRAY && msg.via.array.size >= 4) {
                    call_id = msg.via.array.ptr[1].as<uint32_t>();
                    LOG_INFO("Processing RPC call ID: {}", call_id);
                    
                    // Extract method name for logs
                    std::string method = msg.via.array.ptr[2].as<std::string>();
                    LOG_INFO("RPC method: '{}', args count: {}", method, 
                        msg.via.array.size > 3 ? msg.via.array.ptr[3].via.array.size : 0);
                }
            } catch(...) {
                LOG_WARN("Could not extract call details from message");
            }

            LOG_INFO("Processing request directly (message mode)");
            
            // Dispatch and handle response
            auto resp = disp_->dispatch(msg, suppress_exceptions_);

            // First, if the response is disabled, don't send anything
            if (!rpc::this_handler().resp_enabled_) {
                LOG_INFO("Response disabled, not sending anything back");
                return;
            }

            // Handle errors and special responses
            if (!rpc::this_handler().error_.get().is_nil()) {
                LOG_WARN("Handler reported error");
                resp.capture_error(rpc::this_handler().error_);
            } else if (!rpc::this_handler().resp_.get().is_nil()) {
                LOG_INFO("Handler provided special response");
                resp.capture_result(rpc::this_handler().resp_);
            }

            // Send response synchronously for message mode
            if (!resp.is_empty()) {
                LOG_INFO("Sending response for call ID: {}", call_id);
                auto resp_buf = resp.get_data();
                
                // Enhanced debug logging of response content
                if (enable_pipe_debug || enable_message_tracking) {
                    std::string hex_dump;
                    for (size_t i = 0; i < std::min(resp_buf.size(), size_t(64)); ++i) {
                        char buf[8];
                        snprintf(buf, sizeof(buf), "%02x ", (unsigned char)resp_buf.data()[i]);
                        hex_dump += buf;
                    }
                    LOG_INFO("Response content (first 64 bytes): {}", hex_dump);
                }
                
                try {
                    // Synchronous write to ensure complete message is written
                    RPCLIB_ASIO::write(
                        this->socket(), 
                        RPCLIB_ASIO::buffer(resp_buf.data(), resp_buf.size())
                    );
                    LOG_INFO("Response successfully written: {} bytes", resp_buf.size());
                }
                catch (const std::exception& e) {
                    LOG_ERROR("Error writing response: {}", e.what());
                    std::cerr << "Write error: " << e.what() << std::endl;
                }
            }

            // Handle session exit request
            if (rpc::this_session().exit_) {
                LOG_WARN("Session exit requested from a handler.");
                this->close();
                return;
            }

            // Handle server stop request
            if (rpc::this_server().stopping()) {
                LOG_WARN("Server exit requested from a handler.");
                parent_->close_sessions();
                return;
            }
        }
        catch (const std::exception& e) {
            LOG_ERROR("Exception during direct message processing: {}", e.what());
            std::cerr << "Processing error: " << e.what() << std::endl;
        }
        
        return;
    }
    
    // Standard async processing for non-message mode
    io_->post([this, self, msg, z = std::shared_ptr<RPCLIB_MSGPACK::zone>(result.zone().release())]() {
        rpc::this_handler().clear();
        rpc::this_session().clear();
        rpc::this_session().set_id(reinterpret_cast<rpc::session_id_t>(this));
        rpc::this_server().cancel_stop();

        LOG_INFO("Processing request (standard mode)");
        auto resp = disp_->dispatch(msg, suppress_exceptions_);

        // There are various things that decide what to send
        // as a response. They have a precedence.

        // First, if the response is disabled, that wins
        if (!rpc::this_handler().resp_enabled_) {
            LOG_INFO("Response disabled, not sending anything back");
            return;
        }

        // Second, if there is an error set, we send that
        // and only third, if there is a special response, we use it
        if (!rpc::this_handler().error_.get().is_nil()) {
            LOG_WARN("There was an error set in the handler");
            resp.capture_error(rpc::this_handler().error_);
        } else if (!rpc::this_handler().resp_.get().is_nil()) {
            LOG_WARN("There was a special result set in the handler");
            resp.capture_result(rpc::this_handler().resp_);
        }

        if (!resp.is_empty()) {
            LOG_INFO("Sending response back to client");
#ifdef _MSC_VER
            // doesn't compile otherwise.
            this->write_strand().post(
                [=]() { this->write(resp.get_data()); });
#else
            this->write_strand().post(
                [this, self, resp, z]() { this->write(resp.get_data()); });
#endif
        }

        if (rpc::this_session().exit_) {
            LOG_WARN("Session exit requested from a handler.");
            // posting through the strand so this comes after
            // the previous write
            this->write_strand().post([this]() { this->close(); });
        }

        if (rpc::this_server().stopping()) {
            LOG_WARN("Server exit requested from a handler.");
            // posting through the strand so this comes after
            // the previous write
            this->write_strand().post(
                [this]() { parent_->close_sessions(); });
        }
    });
}

// Message mode read implementation for all socket types - enhanced version
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
                    // Process the message directly
                    process_raw_message(read_buffer_.data(), bytes_read);
                    
                    // Prepare for next read
                    if (!this->is_closed()) {
                        // Clear read buffer for next operation
                        std::fill(read_buffer_.begin(), read_buffer_.end(), 0);
                        
                        // Continue reading
                        do_read_message_mode();
                    }
                }
                catch (const std::exception& e) {
                    LOG_ERROR("Exception during message mode read: {}", e.what());
                    std::cerr << "Message mode read error: " << e.what() << std::endl;
                    
                    // Try to recover and continue reading
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

#ifdef _WIN32
// Specialized implementation for Windows Named Pipes
template<>
void server_pipe_session<RPCLIB_ASIO::windows::stream_handle>::do_read() {
    if (message_mode_) {
        // Use message mode read implementation
        do_read_message_mode();
        return;
    }
    
    LOG_INFO("Starting read operation on Windows Named Pipe (byte mode)");
    
    // Explicitly specify from which base class to call shared_from_this
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
                    // Debug logging of received data
                    if (enable_pipe_debug || enable_message_tracking) {
                        std::string hex_dump;
                        for (size_t i = 0; i < std::min(length, size_t(64)); ++i) {
                            char buf[8];
                            snprintf(buf, sizeof(buf), "%02x ", (unsigned char)read_buffer_[i]);
                            hex_dump += buf;
                        }
                        LOG_INFO("Received data (first 64 bytes): {}", hex_dump);
                    }
                    
                    // Add data to unpacker
                    pac_.reserve_buffer(length);
                    memcpy(pac_.buffer(), read_buffer_.data(), length);
                    pac_.buffer_consumed(length);
                    
                    // Process all complete messages
                    RPCLIB_MSGPACK::object_handle result;
                    while (pac_.next(result) && !this->is_closed()) {
                        LOG_INFO("Processing message");
                        process_message(result);
                    }
                    
                    // Prepare for next read
                    if (!this->is_closed()) {
                        // Reserve more buffer if needed
                        if (pac_.buffer_capacity() < pipe_default_buffer_size) {
                            LOG_INFO("Reserving more buffer: {}", pipe_default_buffer_size);
                            pac_.reserve_buffer(pipe_default_buffer_size);
                        }
                        
                        // Clear read buffer for next operation
                        std::fill(read_buffer_.begin(), read_buffer_.end(), 0);
                        
                        // Continue reading
                        do_read();
                    }
                }
                catch (const std::exception& e) {
                    LOG_ERROR("Exception during Windows Named Pipe read processing: {}", e.what());
                    std::cerr << "Exception in pipe read: " << e.what() << std::endl;
                    
                    // Try to recover and continue reading
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
// Regular implementation for non-Windows platforms
template<typename SocketType>
void server_pipe_session<SocketType>::do_read() {
    if (message_mode_) {
        // Use message mode read implementation
        do_read_message_mode();
        return;
    }
    
    // Explicitly specify from which base class to call shared_from_this
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
                    // resizing strategy: if the remaining buffer size is
                    // less than the maximum bytes requested from asio,
                    // then request max_read_bytes. This prompts the unpacker
                    // to resize its buffer doubling its size
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
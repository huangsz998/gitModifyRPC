#pragma once

#include "rpc/detail/server_pipe_session.h"

#include "rpc/config.h"
#include "rpc/server.h"
#include "rpc/this_handler.h"
#include "rpc/this_server.h"
#include "rpc/this_session.h"

#include "rpc/detail/log.h"

namespace rpc {
namespace detail {

static constexpr std::size_t pipe_default_buffer_size =
    rpc::constants::DEFAULT_BUFFER_SIZE;

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
      suppress_exceptions_(suppress_exceptions) {
    pac_.reserve_buffer(pipe_default_buffer_size);
}

template<typename SocketType>
void server_pipe_session<SocketType>::start() {
    do_read();
}

template<typename SocketType>
void server_pipe_session<SocketType>::close() {
    LOG_INFO("Closing pipe session.");
    async_writer<SocketType>::close();

    // 使用类名明确指定shared_from_this
    auto self = std::static_pointer_cast<server_pipe_session<SocketType>>(
        async_writer<SocketType>::shared_from_this());
        
    this->write_strand().post([this, self]() {
        parent_->close_pipe_session(self);
    });
}

template<typename SocketType>
void server_pipe_session<SocketType>::do_read() {
    // 明确指定从哪个基类调用shared_from_this
    auto self = std::static_pointer_cast<server_pipe_session<SocketType>>(
        async_writer<SocketType>::shared_from_this());
        
    constexpr std::size_t max_read_bytes = pipe_default_buffer_size;
    this->socket().async_read_some(
        RPCLIB_ASIO::buffer(pac_.buffer(), pipe_default_buffer_size),
        read_strand_.wrap([this, self, max_read_bytes](std::error_code ec,
                                                     std::size_t length) {
            if (this->is_closed()) { return; }
            if (!ec) {
                pac_.buffer_consumed(length);
                RPCLIB_MSGPACK::unpacked result;
                while (pac_.next(result) && !this->is_closed()) {
                    auto msg = result.get();
                    output_buf_.clear();

                    // any worker thread can take this call
                    auto z = std::shared_ptr<RPCLIB_MSGPACK::zone>(
                        result.zone().release());
                    io_->post([this, self, msg, z]() {
                        rpc::this_handler().clear();
                        rpc::this_session().clear();
                        rpc::this_session().set_id(reinterpret_cast<rpc::session_id_t>(this));
                        rpc::this_server().cancel_stop();

                        auto resp = disp_->dispatch(msg, suppress_exceptions_);

                        // There are various things that decide what to send
                        // as a response. They have a precedence.

                        // First, if the response is disabled, that wins
                        // So You Get Nothing, You Lose! Good Day Sir!
                        if (!rpc::this_handler().resp_enabled_) {
                            return;
                        }

                        // Second, if there is an error set, we send that
                        // and only third, if there is a special response, we
                        // use it
                        if (!rpc::this_handler().error_.get().is_nil()) {
                            LOG_WARN("There was an error set in the handler");
                            resp.capture_error(rpc::this_handler().error_);
                        } else if (!rpc::this_handler().resp_.get().is_nil()) {
                            LOG_WARN("There was a special result set in the "
                                     "handler");
                            resp.capture_result(rpc::this_handler().resp_);
                        }

                        if (!resp.is_empty()) {
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

} /* detail */
} /* rpc */
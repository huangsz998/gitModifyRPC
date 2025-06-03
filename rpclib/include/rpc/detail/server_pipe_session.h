#pragma once

#ifndef SERVER_PIPE_SESSION_H
#define SERVER_PIPE_SESSION_H

#include "asio.hpp"
#include <memory>
#include <vector>

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

// 用于Named Pipe的专用会话类
template<typename SocketType>
class server_pipe_session : public async_writer<SocketType> {
public:
    server_pipe_session(server *srv, RPCLIB_ASIO::io_service *io,
                   SocketType socket,
                   std::shared_ptr<dispatcher> disp, bool suppress_exceptions);
    void start();
    void close();

private:
    void do_read();

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
    const bool suppress_exceptions_;
    RPCLIB_CREATE_LOG_CHANNEL(pipe_session)
};

} /* detail */
} /* rpc */

// 模板实现
#include "server_pipe_session.inl"

#endif /* end of include guard: SERVER_PIPE_SESSION_H */
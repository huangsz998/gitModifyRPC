#include "rpc/server.h"

#include <atomic>
#include <memory>
#include <stdexcept>
#include <stdint.h>
#include <thread>

#include "asio.hpp"
#include "format.h"

#include "rpc/detail/dev_utils.h"
#include "rpc/detail/log.h"
#include "rpc/detail/server_session.h"
#include "rpc/detail/server_pipe_session.h" // 新增头文件
#include "rpc/detail/thread_group.h"
#include "rpc/this_server.h"

using namespace rpc::detail;
using RPCLIB_ASIO::ip::tcp;
using namespace RPCLIB_ASIO;

namespace rpc {

struct server::impl {
    // 用于存储连接类型
    enum class conn_type { TCP, NAMED_PIPE };
    
    impl(server *parent, std::string const &address, uint16_t port)
        : parent_(parent),
          io_(),
          acceptor_(io_),
          socket_(io_),
          suppress_exceptions_(false),
          conn_type_(conn_type::TCP) {
        auto ep = tcp::endpoint(ip::address::from_string(address), port);
        acceptor_.open(ep.protocol());
#ifndef _WIN32
        acceptor_.set_option(tcp::acceptor::reuse_address(true));
#endif // !_WIN32
        acceptor_.bind(ep);
        acceptor_.listen();
    }

    impl(server *parent, uint16_t port)
        : parent_(parent),
          io_(),
          acceptor_(io_),
          socket_(io_),
          suppress_exceptions_(false),
          conn_type_(conn_type::TCP) {
        auto ep = tcp::endpoint(tcp::v4(), port);
        acceptor_.open(ep.protocol());
#ifndef _WIN32
        acceptor_.set_option(tcp::acceptor::reuse_address(true));
#endif // !_WIN32
        acceptor_.bind(ep);
        acceptor_.listen();
    }
    
    // Named Pipe 构造函数
    impl(server *parent, std::string const &pipe_name)
        : parent_(parent),
          io_(),
          acceptor_(io_),
          socket_(io_),
          suppress_exceptions_(false),
          conn_type_(conn_type::NAMED_PIPE),
          pipe_name_(pipe_name) {
#ifdef _WIN32
        // Windows平台下的Named Pipe实现
        pipe_acceptor_.reset(new local::windows::stream_protocol::acceptor(
            io_, local::windows::stream_protocol::endpoint("\\\\.\\pipe\\" + pipe_name)));
        pipe_socket_.reset(new local::windows::stream_protocol::socket(io_));
#else
        // Unix平台下的Named Pipe实现 (Unix Domain Socket)
        ::unlink(pipe_name.c_str()); // 删除可能存在的旧socket文件
        pipe_acceptor_.reset(new local::stream_protocol::acceptor(
            io_, local::stream_protocol::endpoint(pipe_name)));
        pipe_socket_.reset(new local::stream_protocol::socket(io_));
#endif
        LOG_INFO("Created server with named pipe: {}", pipe_name);
    }

    void start_accept() {
        if (conn_type_ == conn_type::TCP) {
            acceptor_.async_accept(socket_, [this](std::error_code ec) {
                if (!ec) {
                    auto ep = socket_.remote_endpoint();
                    LOG_INFO("Accepted TCP connection from {}:{}", ep.address(),
                             ep.port());
                    auto s = std::make_shared<server_session>(
                        parent_, &io_, std::move(socket_), parent_->disp_,
                        suppress_exceptions_);
                    s->start();
                    std::unique_lock<std::mutex> lock(sessions_mutex_);
                    sessions_.push_back(s);
                    tcp_sessions_.push_back(s);
                } else {
                    LOG_ERROR("Error while accepting TCP connection: {}", ec);
                }
                if (!this_server().stopping())
                    start_accept();
            });
        } else { // NAMED_PIPE
#ifdef _WIN32
            pipe_acceptor_->async_accept(*pipe_socket_, [this](std::error_code ec) {
                if (!ec) {
                    LOG_INFO("Accepted Named Pipe connection");
                    using PipeSocketType = local::windows::stream_protocol::socket;
                    auto s = std::make_shared<server_pipe_session<PipeSocketType>>(
                        parent_, &io_, std::move(*pipe_socket_), parent_->disp_,
                        suppress_exceptions_);
                    s->start();
                    std::unique_lock<std::mutex> lock(sessions_mutex_);
                    sessions_.push_back(s);
                    pipe_sessions_.push_back(s);
                    // 创建新的socket用于下一个连接
                    pipe_socket_.reset(new local::windows::stream_protocol::socket(io_));
                } else {
                    LOG_ERROR("Error while accepting Named Pipe connection: {}", ec);
                }
                if (!this_server().stopping())
                    start_accept();
            });
#else
            pipe_acceptor_->async_accept(*pipe_socket_, [this](std::error_code ec) {
                if (!ec) {
                    LOG_INFO("Accepted Unix Domain Socket connection");
                    using PipeSocketType = local::stream_protocol::socket;
                    auto s = std::make_shared<server_pipe_session<PipeSocketType>>(
                        parent_, &io_, std::move(*pipe_socket_), parent_->disp_,
                        suppress_exceptions_);
                    s->start();
                    std::unique_lock<std::mutex> lock(sessions_mutex_);
                    sessions_.push_back(s);
                    pipe_sessions_.push_back(s);
                    // 创建新的socket用于下一个连接
                    pipe_socket_.reset(new local::stream_protocol::socket(io_));
                } else {
                    LOG_ERROR("Error while accepting Unix Domain Socket connection: {}", ec);
                }
                if (!this_server().stopping())
                    start_accept();
            });
#endif
        }
    }

    void close_sessions() {
        std::unique_lock<std::mutex> lock(sessions_mutex_);
        // 分别复制TCP和管道会话列表，避免void*指针的类型问题
        auto tcp_sessions_copy = tcp_sessions_;
        auto pipe_sessions_copy = pipe_sessions_;
        sessions_.clear();
        tcp_sessions_.clear();
        pipe_sessions_.clear();
        lock.unlock();

        // 关闭所有TCP会话
        for (auto &session : tcp_sessions_copy) {
            session->close();
        }
        
        // 关闭所有管道会话(这里不使用void*指针)
        // 此处省略管道会话的关闭，实际会在各个会话被销毁时自动处理

        if (this_server().stopping()) {
            if (conn_type_ == conn_type::TCP) {
                acceptor_.cancel();
            } else {
#ifdef _WIN32
                pipe_acceptor_->cancel();
#else
                pipe_acceptor_->cancel();
                ::unlink(pipe_name_.c_str()); // 清理Unix Domain Socket文件
#endif
            }
        }
    }

    void stop() {
        io_.stop();
        loop_workers_.join_all();
        
        if (conn_type_ == conn_type::NAMED_PIPE) {
#ifndef _WIN32
            ::unlink(pipe_name_.c_str()); // 清理Unix Domain Socket文件
#endif
        }
    }

    // 用于TCP session的close方法
    void close_tcp_session(std::shared_ptr<server_session> const &s) {
        std::unique_lock<std::mutex> lock(sessions_mutex_);
        // 从TCP sessions列表中删除
        auto it_tcp = std::find(begin(tcp_sessions_), end(tcp_sessions_), s);
        if (it_tcp != end(tcp_sessions_)) {
            tcp_sessions_.erase(it_tcp);
        }
        
        // 从通用sessions列表中删除
        auto it = std::find(begin(sessions_), end(sessions_), static_cast<std::shared_ptr<void>>(s));
        if (it != end(sessions_)) {
            sessions_.erase(it);
        }
    }
    
    // 用于Pipe session的close方法
    template<typename SocketType>
    void close_pipe_session(std::shared_ptr<server_pipe_session<SocketType>> const &s) {
        std::unique_lock<std::mutex> lock(sessions_mutex_);
        
        // 从pipe sessions列表中删除，通过确切类型转换比较指针
        for (auto it = pipe_sessions_.begin(); it != pipe_sessions_.end(); ++it) {
            auto ptr = std::static_pointer_cast<server_pipe_session<SocketType>>(*it);
            if (ptr.get() == s.get()) {
                pipe_sessions_.erase(it);
                break;
            }
        }
        
        // 从通用sessions列表中删除
        for (auto it = sessions_.begin(); it != sessions_.end(); ++it) {
            // 直接比较指针地址
            if (it->get() == s.get()) {
                sessions_.erase(it);
                break;
            }
        }
    }

    unsigned short port() const { 
        if (conn_type_ == conn_type::TCP) {
            return acceptor_.local_endpoint().port(); 
        }
        return 0; // Named Pipe模式下返回0
    }
    
    std::string pipe_name() const {
        return pipe_name_;
    }
    
    server::connection_type get_connection_type() const {
        return conn_type_ == conn_type::TCP ? 
               server::connection_type::TCP : 
               server::connection_type::NAMED_PIPE;
    }

    server *parent_;
    io_service io_;
    ip::tcp::acceptor acceptor_;
    ip::tcp::socket socket_;
    
    // Named Pipe相关成员
#ifdef _WIN32
    std::unique_ptr<local::windows::stream_protocol::acceptor> pipe_acceptor_;
    std::unique_ptr<local::windows::stream_protocol::socket> pipe_socket_;
#else
    std::unique_ptr<local::stream_protocol::acceptor> pipe_acceptor_;
    std::unique_ptr<local::stream_protocol::socket> pipe_socket_;
#endif
    std::string pipe_name_;
    
    rpc::detail::thread_group loop_workers_;
    std::vector<std::shared_ptr<void>> sessions_;        // 所有会话的混合容器
    std::vector<std::shared_ptr<server_session>> tcp_sessions_; // TCP会话容器
    std::vector<std::shared_ptr<void>> pipe_sessions_;   // Named Pipe会话容器
    std::atomic_bool suppress_exceptions_;
    conn_type conn_type_;
    RPCLIB_CREATE_LOG_CHANNEL(server)
    std::mutex sessions_mutex_;
};

RPCLIB_CREATE_LOG_CHANNEL(server)

server::server(uint16_t port)
    : pimpl(new server::impl(this, port)),
      disp_(std::make_shared<dispatcher>()) {
    LOG_INFO("Created server on localhost:{}", port);
    pimpl->start_accept();
}

server::server(server &&other) noexcept { *this = std::move(other); }

server::server(std::string const &address, uint16_t port)
    : pimpl(new server::impl(this, address, port)),
      disp_(std::make_shared<dispatcher>()) {
    LOG_INFO("Created server on address {}:{}", address, port);
    pimpl->start_accept();
}

// 新增的Named Pipe构造函数实现
server::server(std::string const &pipe_name)
    : pimpl(new server::impl(this, pipe_name)),
      disp_(std::make_shared<dispatcher>()) {
    pimpl->start_accept();
}

server::~server() {
    if (pimpl) {
        pimpl->stop();
    }
}

server &server::operator=(server &&other) {
    if (this != &other) {
        pimpl = std::move(other.pimpl);
        other.pimpl = nullptr;
        disp_ = std::move(other.disp_);
        other.disp_ = nullptr;
    }
    return *this;
}

void server::suppress_exceptions(bool suppress) {
    pimpl->suppress_exceptions_ = suppress;
}

void server::run() { pimpl->io_.run(); }

void server::async_run(std::size_t worker_threads) {
    pimpl->loop_workers_.create_threads(worker_threads, [this]() {
        name_thread("server");
        LOG_INFO("Starting");
        pimpl->io_.run();
        LOG_INFO("Exiting");
    });
}

void server::stop() { pimpl->stop(); }

unsigned short server::port() const { return pimpl->port(); }

std::string server::pipe_name() const { return pimpl->pipe_name(); }

server::connection_type server::get_connection_type() const {
    return pimpl->get_connection_type();
}

void server::close_sessions() { pimpl->close_sessions(); }

// 原有的TCP会话关闭方法
void server::close_session(std::shared_ptr<detail::server_session> const &s) {
    pimpl->close_tcp_session(s);
}

template<typename SocketType>
void server::close_pipe_session(std::shared_ptr<detail::server_pipe_session<SocketType>> const &s) {
    pimpl->close_pipe_session(s);
}

// 显式实例化模板
#ifdef _WIN32
template void server::close_pipe_session<local::windows::stream_protocol::socket>(
    std::shared_ptr<detail::server_pipe_session<local::windows::stream_protocol::socket>> const &);
#else
template void server::close_pipe_session<local::stream_protocol::socket>(
    std::shared_ptr<detail::server_pipe_session<local::stream_protocol::socket>> const &);
#endif

} // namespace rpc
#include "rpc/server.h"

#include <atomic>
#include <memory>
#include <stdexcept>
#include <stdint.h>
#include <thread>
#include <algorithm>
#include <iostream>

#include "asio.hpp"
#include "format.h"

#include "rpc/detail/dev_utils.h"
#include "rpc/detail/log.h"
#include "rpc/detail/server_session.h"
#include "rpc/detail/server_pipe_session.h"
#include "rpc/detail/thread_group.h"
#include "rpc/this_server.h"

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

using namespace rpc::detail;
using RPCLIB_ASIO::ip::tcp;
using namespace RPCLIB_ASIO;

namespace rpc {
extern bool enable_pipe_debug;
extern bool enable_message_tracking;

struct server::impl {
    enum class conn_type { TCP, NAMED_PIPE };

    impl(server *parent, std::string const &address, uint16_t port)
            : parent_(parent),
              io_(),
              acceptor_(io_),
              socket_(io_),
              suppress_exceptions_(false),
              conn_type_(conn_type::TCP),
              pipe_name_(""),
              use_named_pipe_message_mode_(false) {
        auto ep = tcp::endpoint(ip::address::from_string(address), port);
        acceptor_.open(ep.protocol());
#ifndef _WIN32
        acceptor_.set_option(tcp::acceptor::reuse_address(true));
#endif
        acceptor_.bind(ep);
        acceptor_.listen();
    }

    impl(server *parent, uint16_t port)
            : parent_(parent),
              io_(),
              acceptor_(io_),
              socket_(io_),
              suppress_exceptions_(false),
              conn_type_(conn_type::TCP),
              pipe_name_(""),
              use_named_pipe_message_mode_(false) {
        auto ep = tcp::endpoint(tcp::v4(), port);
        acceptor_.open(ep.protocol());
#ifndef _WIN32
        acceptor_.set_option(tcp::acceptor::reuse_address(true));
#endif
        acceptor_.bind(ep);
        acceptor_.listen();
    }

#ifndef _WIN32
    // Unix Domain Socket
    impl(server *parent, std::string const &pipe_name)
            : parent_(parent),
              io_(),
              acceptor_(io_),
              socket_(io_),
              suppress_exceptions_(false),
              conn_type_(conn_type::NAMED_PIPE),
              pipe_name_(pipe_name),
              use_named_pipe_message_mode_(false) {
        ::unlink(pipe_name.c_str());
        pipe_acceptor_.reset(new local::stream_protocol::acceptor(
                io_, local::stream_protocol::endpoint(pipe_name)));
        pipe_socket_.reset(new local::stream_protocol::socket(io_));
        LOG_INFO("Created server with Unix Domain Socket: {}", pipe_name);
    }
#else
    // Windows Named Pipe
    impl(server *parent, std::string const &pipe_name)
     : parent_(parent),
       io_(),
       acceptor_(io_),
       socket_(io_),
       suppress_exceptions_(false),
       conn_type_(conn_type::NAMED_PIPE),
       pipe_name_(pipe_name),
       use_named_pipe_message_mode_(true) {
         full_pipe_name_ = "\\\\.\\pipe\\" + pipe_name;
         LOG_INFO("Creating Windows Named Pipe: {}", full_pipe_name_);
         std::cout << "[server.cc] Creating Windows Named Pipe: " << full_pipe_name_ << std::endl;
         // Only the first pipe instance uses FILE_FLAG_FIRST_PIPE_INSTANCE
         pipe_handle_ = CreateNamedPipeA(
             full_pipe_name_.c_str(),
             PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED | FILE_FLAG_FIRST_PIPE_INSTANCE,
             PIPE_TYPE_MESSAGE | PIPE_READMODE_MESSAGE,
             PIPE_UNLIMITED_INSTANCES,
             8192, 8192, 0, NULL);
         if (pipe_handle_ == INVALID_HANDLE_VALUE) {
             DWORD error = GetLastError();
             std::string err_msg = RPCLIB_FMT::format(
                 "Failed to create Windows Named Pipe: {}, error code: {}",
                 full_pipe_name_, error);
             LOG_ERROR(err_msg);
             std::cout << "[server.cc] Failed to create Windows Named Pipe: error=" << error << std::endl;
             throw std::runtime_error(err_msg);
         }
         LOG_INFO("Created server with Windows Named Pipe: {}", full_pipe_name_);
         std::cout << "[server.cc] Created server with Windows Named Pipe: " << full_pipe_name_ << std::endl;
     }
#endif

#ifdef _WIN32
    void convert_to_named_pipe(std::string const &pipe_name) {
        acceptor_.close();
        socket_.close();
        conn_type_ = conn_type::NAMED_PIPE;
        pipe_name_ = pipe_name;
        use_named_pipe_message_mode_ = true;
        full_pipe_name_ = "\\\\.\\pipe\\" + pipe_name;
        LOG_INFO("Creating Windows Named Pipe: {}", full_pipe_name_);
        std::cout << "[server.cc] Creating Windows Named Pipe: " << full_pipe_name_ << std::endl;
        // Only the first pipe instance uses FILE_FLAG_FIRST_PIPE_INSTANCE
        pipe_handle_ = CreateNamedPipeA(
            full_pipe_name_.c_str(),
            PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED | FILE_FLAG_FIRST_PIPE_INSTANCE,
            PIPE_TYPE_MESSAGE | PIPE_READMODE_MESSAGE,
            PIPE_UNLIMITED_INSTANCES,
            8192, 8192, 0, NULL);
        if (pipe_handle_ == INVALID_HANDLE_VALUE) {
            DWORD error = GetLastError();
            std::string err_msg = RPCLIB_FMT::format(
                "Failed to create Windows Named Pipe: {}, error code: {}",
                full_pipe_name_, error);
            LOG_ERROR(err_msg);
            std::cout << "[server.cc] Failed to create Windows Named Pipe: error=" << error << std::endl;
            throw std::runtime_error(err_msg);
        }
        LOG_INFO("Created server with Windows Named Pipe: {}", full_pipe_name_);
        std::cout << "[server.cc] Created server with Windows Named Pipe: " << full_pipe_name_ << std::endl;
    }

    void set_named_pipe_options() {
        use_named_pipe_message_mode_ = true;
        LOG_INFO("Server configured to use named pipe message mode");
        std::cout << "[server.cc] Server configured to use named pipe message mode" << std::endl;
        if (enable_pipe_debug) {
            LOG_INFO("Pipe debugging is enabled for server");
            std::cout << "[server.cc] Pipe debugging is enabled for server" << std::endl;
        }
    }
#endif

    void start_accept() {
        if (conn_type_ == conn_type::TCP) {
            acceptor_.async_accept(socket_, [this](std::error_code ec) {
                if (!ec) {
                    auto ep = socket_.remote_endpoint();
                    LOG_INFO("Accepted TCP connection from {}:{}", ep.address(), ep.port());
                    auto s = std::make_shared<server_session>( parent_, &io_, std::move(socket_), parent_->disp_, suppress_exceptions_);
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
        }
#ifdef _WIN32
        else {
            if (pipe_handle_ == INVALID_HANDLE_VALUE) {
                LOG_ERROR("Invalid pipe handle when trying to accept connection");
                std::cerr << "[server.cc] Invalid pipe handle when trying to accept connection" << std::endl;
                return;
            }
            // Use async/overlapped approach for ConnectNamedPipe
            io_.post([this]() {
                try {
                    LOG_INFO("Waiting for client connection on pipe: {}", pipe_name_);
                    std::cout << "[server.cc] Waiting for client connection on pipe: " << pipe_name_ << std::endl;

                    OVERLAPPED ov = {};
                    HANDLE hEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
                    ov.hEvent = hEvent;
                    BOOL connected = ConnectNamedPipe(pipe_handle_, &ov);
                    DWORD last_error = GetLastError();
                    if (!connected && last_error == ERROR_IO_PENDING) {
                        DWORD wait_result = WaitForSingleObject(hEvent, INFINITE);
                        if (wait_result == WAIT_OBJECT_0) {
                            last_error = 0;
                        }
                    }
                    CloseHandle(hEvent);

                    std::cout << "[server.cc] ConnectNamedPipe result: connected=" << connected << " last_error=" << last_error << std::endl;
                    if (last_error == 0 || last_error == ERROR_PIPE_CONNECTED) {
                        LOG_INFO("Client connected to Windows Named Pipe");
                        std::cout << "[server.cc] Client connected to pipe" << std::endl;
                        HANDLE session_handle = pipe_handle_;

                        DWORD mode = PIPE_READMODE_MESSAGE;
                        if (!SetNamedPipeHandleState(session_handle, &mode, NULL, NULL)) {
                            DWORD error = GetLastError();
                            LOG_ERROR("Failed to set pipe mode to message mode: {}", error);
                            std::cout << "[server.cc] Failed to set pipe mode to message mode: error=" << error << std::endl;
                            CloseHandle(session_handle);
                            throw std::runtime_error("Failed to set pipe mode");
                        }
                        try {
                            std::cout << "[server.cc] Creating server_pipe_session for connected client, handle=" << session_handle << std::endl;
                            auto session = std::make_shared<server_pipe_session<RPCLIB_ASIO::windows::stream_handle>>(
                                parent_, &io_, RPCLIB_ASIO::windows::stream_handle(io_, session_handle),
                                parent_->disp_, suppress_exceptions_);
                            LOG_INFO("server.cc: server_pipe_session created");

                            if (use_named_pipe_message_mode_) {
                                session->set_message_mode(true);
                                LOG_INFO("server.cc: Created pipe session with message mode enabled");
                            }

                            {
                                std::unique_lock<std::mutex> lock(sessions_mutex_);
                                sessions_.push_back(session);
                                win_pipe_sessions_.push_back(session);
                                LOG_INFO("server.cc: session pushed to sessions_ and win_pipe_sessions_");
                            }

                            session->start();
                            LOG_INFO("server.cc: Pipe session start() called");
                        } catch (const std::exception& ex) {
                            LOG_ERROR("Exception creating pipe session: {}", ex.what());
                            std::cerr << "[server.cc] Exception creating pipe session: " << ex.what() << std::endl;
                        }
                        // Now create the next pipe instance for the next connection, WITHOUT FILE_FLAG_FIRST_PIPE_INSTANCE!
                        pipe_handle_ = CreateNamedPipeA(
                            full_pipe_name_.c_str(),
                            PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED,  // No FILE_FLAG_FIRST_PIPE_INSTANCE here!
                            PIPE_TYPE_MESSAGE | PIPE_READMODE_MESSAGE,
                            PIPE_UNLIMITED_INSTANCES,
                            8192, 8192, 0, NULL);
                        if (pipe_handle_ == INVALID_HANDLE_VALUE) {
                            DWORD error = GetLastError();
                            LOG_ERROR("Failed to create new pipe instance: {}", error);
                            std::cout << "[server.cc] Failed to create new pipe instance: error=" << error << std::endl;
                        }
                        if (!this_server().stopping())
                            start_accept();
                    } else {
                        std::cout << "[server.cc] ConnectNamedPipe failed, last_error=" << last_error << std::endl;
                        if (!this_server().stopping())
                            start_accept();
                    }
                } catch (const std::exception &e) {
                    LOG_ERROR("Exception in pipe acceptance: {}", e.what());
                    std::cerr << "[server.cc] Pipe acceptance exception: " << e.what() << std::endl;
                    if (pipe_handle_ != INVALID_HANDLE_VALUE) {
                        DisconnectNamedPipe(pipe_handle_);
                        CloseHandle(pipe_handle_);
                        pipe_handle_ = CreateNamedPipeA(
                            full_pipe_name_.c_str(),
                            PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED, // No FILE_FLAG_FIRST_PIPE_INSTANCE here!
                            PIPE_TYPE_MESSAGE | PIPE_READMODE_MESSAGE,
                            PIPE_UNLIMITED_INSTANCES,
                            8192, 8192, 0, NULL);
                    }
                    if (!this_server().stopping())
                        io_.post([this]() { start_accept(); });
                }
            });
        }
#else
        else {
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
                    unix_pipe_sessions_.push_back(s);
                    pipe_socket_.reset(new local::stream_protocol::socket(io_));
                } else {
                    LOG_ERROR("Error while accepting Unix Domain Socket connection: {}", ec);
                }
                if (!this_server().stopping())
                    start_accept();
            });
        }
#endif
    }

    void close_sessions() {
        std::unique_lock<std::mutex> lock(sessions_mutex_);
        auto tcp_sessions_copy = tcp_sessions_;
#ifdef _WIN32
        auto win_pipe_sessions_copy = win_pipe_sessions_;
#else
        auto unix_pipe_sessions_copy = unix_pipe_sessions_;
#endif
        sessions_.clear();
        tcp_sessions_.clear();
#ifdef _WIN32
        win_pipe_sessions_.clear();
#else
        unix_pipe_sessions_.clear();
#endif
        lock.unlock();
        for (auto &session : tcp_sessions_copy) session->close();
#ifdef _WIN32
        for (auto &session : win_pipe_sessions_copy) session->close();
#else
        for (auto &session : unix_pipe_sessions_copy) session->close();
#endif
        if (this_server().stopping()) {
            if (conn_type_ == conn_type::TCP) {
                acceptor_.cancel();
            } else {
#ifdef _WIN32
                if (pipe_handle_ != INVALID_HANDLE_VALUE) {
                    DisconnectNamedPipe(pipe_handle_);
                    CloseHandle(pipe_handle_);
                    pipe_handle_ = INVALID_HANDLE_VALUE;
                }
#else
                pipe_acceptor_->cancel();
                ::unlink(pipe_name_.c_str());
#endif
            }
        }
    }

    void stop() {
        io_.stop();
        loop_workers_.join_all();
        if (conn_type_ == conn_type::NAMED_PIPE) {
#ifdef _WIN32
            if (pipe_handle_ != INVALID_HANDLE_VALUE) {
                DisconnectNamedPipe(pipe_handle_);
                CloseHandle(pipe_handle_);
                pipe_handle_ = INVALID_HANDLE_VALUE;
            }
#else
            ::unlink(pipe_name_.c_str());
#endif
        }
    }

    void close_tcp_session(std::shared_ptr<server_session> const &s) {
        std::unique_lock<std::mutex> lock(sessions_mutex_);
        auto it_tcp = std::find(begin(tcp_sessions_), end(tcp_sessions_), s);
        if (it_tcp != end(tcp_sessions_)) tcp_sessions_.erase(it_tcp);
        auto it = std::find(begin(sessions_), end(sessions_), static_cast<std::shared_ptr<void>>(s));
        if (it != end(sessions_)) sessions_.erase(it);
    }
#ifdef _WIN32
    void close_pipe_session(std::shared_ptr<server_pipe_session<RPCLIB_ASIO::windows::stream_handle>> const &s) {
        std::unique_lock<std::mutex> lock(sessions_mutex_);
        auto it_pipe = std::find(begin(win_pipe_sessions_), end(win_pipe_sessions_), s);
        if (it_pipe != end(win_pipe_sessions_)) win_pipe_sessions_.erase(it_pipe);
        auto it = std::find(begin(sessions_), end(sessions_), static_cast<std::shared_ptr<void>>(s));
        if (it != end(sessions_)) sessions_.erase(it);
    }
#else
    void close_pipe_session(std::shared_ptr<server_pipe_session<local::stream_protocol::socket>> const &s) {
        std::unique_lock<std::mutex> lock(sessions_mutex_);
        auto it_pipe = std::find(begin(unix_pipe_sessions_), end(unix_pipe_sessions_), s);
        if (it_pipe != end(unix_pipe_sessions_)) unix_pipe_sessions_.erase(it_pipe);
        auto it = std::find(begin(sessions_), end(sessions_), static_cast<std::shared_ptr<void>>(s));
        if (it != end(sessions_)) sessions_.erase(it);
    }
#endif
    unsigned short port() const {
        if (conn_type_ == conn_type::TCP) return acceptor_.local_endpoint().port();
        return 0;
    }
    std::string pipe_name() const { return pipe_name_; }
    server::connection_type get_connection_type() const { return conn_type_ == conn_type::TCP ?
            server::connection_type::TCP :
            server::connection_type::NAMED_PIPE;
    }

    server *parent_;
    io_service io_;
    ip::tcp::acceptor acceptor_;
    ip::tcp::socket socket_;
#ifdef _WIN32
    HANDLE pipe_handle_ = INVALID_HANDLE_VALUE;
    std::string full_pipe_name_;
#else
    std::unique_ptr<local::stream_protocol::acceptor> pipe_acceptor_;
    std::unique_ptr<local::stream_protocol::socket> pipe_socket_;
#endif
    std::string pipe_name_;
    rpc::detail::thread_group loop_workers_;
    std::vector<std::shared_ptr<void>> sessions_;
    std::vector<std::shared_ptr<server_session>> tcp_sessions_;
#ifdef _WIN32
    std::vector<std::shared_ptr<server_pipe_session<RPCLIB_ASIO::windows::stream_handle>>> win_pipe_sessions_;
#else
    std::vector<std::shared_ptr<server_pipe_session<local::stream_protocol::socket>>> unix_pipe_sessions_;
#endif
    std::atomic_bool suppress_exceptions_;
    conn_type conn_type_;
    bool use_named_pipe_message_mode_;
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

server::server(server &&other) noexcept {
    *this = std::move(other);
}

server::server(std::string const &pipe_name)
        : pimpl(new server::impl(this, pipe_name)),
          disp_(std::make_shared<dispatcher>()) {
    LOG_INFO("Created server with Named Pipe: {}", pipe_name);
    pimpl->start_accept();
}

std::shared_ptr<server> server::create_named_pipe_server(const std::string &pipe_name) {
#ifdef _WIN32
    std::shared_ptr<server> srv = std::make_shared<server>(0);
    srv->pimpl->convert_to_named_pipe(pipe_name);
    srv->pimpl->set_named_pipe_options();
    srv->pimpl->start_accept();
    return srv;
#else
    auto srv = std::make_shared<server>(pipe_name);
    return srv;
#endif
}

server::~server() {
    if (pimpl) pimpl->stop();
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
server::connection_type server::get_connection_type() const { return pimpl->get_connection_type(); }
void server::close_sessions() { pimpl->close_sessions(); }
void server::close_session(std::shared_ptr<detail::server_session> const &s) { pimpl->close_tcp_session(s); }
#ifdef _WIN32
template <>
void server::close_pipe_session<RPCLIB_ASIO::windows::stream_handle>(std::shared_ptr<detail::server_pipe_session<RPCLIB_ASIO::windows::stream_handle>> const &s) {
    pimpl->close_pipe_session(s);
}
#else
template <>
void server::close_pipe_session<local::stream_protocol::socket>(std::shared_ptr<detail::server_pipe_session<local::stream_protocol::socket>> const &s) {
    pimpl->close_pipe_session(s);
}
#endif

} // namespace rpc
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
#include "rpc/detail/server_pipe_session.h" // Include header for Named Pipe session
#include "rpc/detail/thread_group.h"
#include "rpc/detail/pipe_utils.h"
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
    // Access global variables defined in client.cc
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
                  use_named_pipe_message_mode_(false) {  // Initialize new flag (NEW)
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
                  use_named_pipe_message_mode_(false) {  // Initialize new flag (NEW)
            auto ep = tcp::endpoint(tcp::v4(), port);
            acceptor_.open(ep.protocol());
#ifndef _WIN32
            acceptor_.set_option(tcp::acceptor::reuse_address(true));
#endif
            acceptor_.bind(ep);
            acceptor_.listen();
        }

#ifndef _WIN32
        // Unix Named Pipe constructor
        impl(server *parent, std::string const &pipe_name)
                : parent_(parent),
                  io_(),
                  acceptor_(io_),
                  socket_(io_),
                  suppress_exceptions_(false),
                  conn_type_(conn_type::NAMED_PIPE),
                  pipe_name_(pipe_name),
                  use_named_pipe_message_mode_(false) {  // Initialize new flag (NEW)

            ::unlink(pipe_name.c_str());
            pipe_acceptor_.reset(new local::stream_protocol::acceptor(
                    io_, local::stream_protocol::endpoint(pipe_name)));
            pipe_socket_.reset(new local::stream_protocol::socket(io_));
            LOG_INFO("Created server with Unix Domain Socket: {}", pipe_name);
        }
#else
        // Windows Named Pipe constructor
        impl(server *parent, std::string const &pipe_name)
        : parent_(parent),
          io_(),
          acceptor_(io_),
          socket_(io_),
          suppress_exceptions_(false),
          conn_type_(conn_type::NAMED_PIPE),
          pipe_name_(pipe_name),
          use_named_pipe_message_mode_(true) {  // Default to message mode (CHANGED)
              
            // Initialize Windows Named Pipe with MESSAGE mode
            full_pipe_name_ = "\\\\.\\pipe\\" + pipe_name;
            LOG_INFO("Creating Windows Named Pipe: {}", full_pipe_name_);
            std::cout << "Creating Windows Named Pipe: " << full_pipe_name_ << std::endl;
            
            pipe_handle_ = CreateNamedPipeA(
                full_pipe_name_.c_str(),
                PIPE_ACCESS_DUPLEX,
                PIPE_TYPE_MESSAGE | PIPE_READMODE_MESSAGE, // MESSAGE mode instead of BYTE mode
                PIPE_UNLIMITED_INSTANCES,
                8192, 8192, 0, NULL);

            if (pipe_handle_ == INVALID_HANDLE_VALUE) {
                DWORD error = GetLastError();
                std::string err_msg = RPCLIB_FMT::format(
                    "Failed to create Windows Named Pipe: {}, error code: {}",
                    full_pipe_name_, error);
                LOG_ERROR(err_msg);
                log_win_error("CreateNamedPipe", error);
                throw std::runtime_error(err_msg);
            }
            LOG_INFO("Created server with Windows Named Pipe: {}", full_pipe_name_);
            std::cout << "Created server with Windows Named Pipe: " << full_pipe_name_ << std::endl;
        }

#endif

#ifdef _WIN32
        // Method to convert to Windows Named Pipe implementation
        void convert_to_named_pipe(std::string const &pipe_name) {
            // Close TCP resources
            acceptor_.close();
            socket_.close();
        
            // Switch to Named Pipe mode
            conn_type_ = conn_type::NAMED_PIPE;
            pipe_name_ = pipe_name;
            use_named_pipe_message_mode_ = true; // Default to message mode (CHANGED)
        
            // Create Windows Named Pipe with MESSAGE mode
            full_pipe_name_ = "\\\\.\\pipe\\" + pipe_name;
            LOG_INFO("Creating Windows Named Pipe: {}", full_pipe_name_);
            std::cout << "Creating Windows Named Pipe: " << full_pipe_name_ << std::endl;
            
            pipe_handle_ = CreateNamedPipeA(
                full_pipe_name_.c_str(),
                PIPE_ACCESS_DUPLEX,
                PIPE_TYPE_MESSAGE | PIPE_READMODE_MESSAGE, // MESSAGE mode instead of BYTE mode
                PIPE_UNLIMITED_INSTANCES,
                8192, 8192, 0, NULL);
        
            if (pipe_handle_ == INVALID_HANDLE_VALUE) {
                DWORD error = GetLastError();
                std::string err_msg = RPCLIB_FMT::format(
                    "Failed to create Windows Named Pipe: {}, error code: {}",
                    full_pipe_name_, error);
                LOG_ERROR(err_msg);
                log_win_error("CreateNamedPipe", error);
                throw std::runtime_error(err_msg);
            }
            LOG_INFO("Created server with Windows Named Pipe: {}", full_pipe_name_);
            std::cout << "Created server with Windows Named Pipe: " << full_pipe_name_ << std::endl;
        }
        
        // New method to set named pipe options (NEW)
        void set_named_pipe_options() {
            // Set message mode flag
            use_named_pipe_message_mode_ = true;
            LOG_INFO("Server configured to use named pipe message mode");
            std::cout << "Server configured to use named pipe message mode" << std::endl;
            
            if (enable_pipe_debug) {
                LOG_INFO("Pipe debugging is enabled for server");
                std::cout << "Pipe debugging is enabled for server" << std::endl;
            }
        }
#endif

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
                // Windows Named Pipe with MESSAGE mode handling
                if (pipe_handle_ == INVALID_HANDLE_VALUE) {
                    LOG_ERROR("Invalid pipe handle when trying to accept connection");
                    std::cerr << "Invalid pipe handle when trying to accept connection" << std::endl;
                    return;
                }

                // Post to io_service to handle pipe connection asynchronously
                io_.post([this]() {
                    try {
                        LOG_INFO("Waiting for client connection on pipe: {}", pipe_name_);
                        std::cout << "Waiting for client connection on pipe: " << pipe_name_ << std::endl;
                        
                        // Use synchronous connection
                        BOOL connected = ConnectNamedPipe(pipe_handle_, NULL);
                        DWORD last_error = GetLastError();

                        if (connected || last_error == ERROR_PIPE_CONNECTED) {
                            LOG_INFO("Client connected to Windows Named Pipe");
                            std::cout << "Client connected to pipe" << std::endl;
                            
                            // Save current pipe handle for this client
                            HANDLE client_handle = pipe_handle_;
                            HANDLE session_handle = INVALID_HANDLE_VALUE;
                            
                            // 创建句柄副本用于会话
                            LOG_INFO("Duplicating pipe handle for session");
                            if (!DuplicateHandle(
                                    GetCurrentProcess(),  // 源进程
                                    client_handle,        // 源句柄
                                    GetCurrentProcess(),  // 目标进程
                                    &session_handle,      // 目标句柄引用
                                    0,                    // 与源相同的访问权限
                                    FALSE,                // 不继承
                                    DUPLICATE_SAME_ACCESS)) { // 相同访问权限
                                
                                DWORD error = GetLastError();
                                log_win_error("DuplicateHandle", error);
                                throw std::runtime_error("Failed to duplicate pipe handle for session");
                            }
                            
                            LOG_INFO("Successfully duplicated handle - Original: {}, Session: {}", 
                                     (void*)client_handle, (void*)session_handle);
                            
                            // Create new pipe instance for next client
                            pipe_handle_ = CreateNamedPipeA(
                                full_pipe_name_.c_str(),
                                PIPE_ACCESS_DUPLEX,
                                PIPE_TYPE_MESSAGE | PIPE_READMODE_MESSAGE,
                                PIPE_UNLIMITED_INSTANCES,
                                8192, 8192, 0, NULL);
                                
                            if (pipe_handle_ == INVALID_HANDLE_VALUE) {
                                DWORD error = GetLastError();
                                LOG_ERROR("Failed to create new pipe instance: {}", error);
                                log_win_error("CreateNamedPipe", error);
                                
                                // 释放不再需要的原始句柄
                                CloseHandle(client_handle);
                            } else {
                                // 原始客户端句柄已不再需要，因为我们有了副本用于会话
                                // 且新实例已创建
                                CloseHandle(client_handle);
                            }
                            
                            // After connection, set the session pipe handle to message mode
                            DWORD mode = PIPE_READMODE_MESSAGE;
                            if (!SetNamedPipeHandleState(session_handle, &mode, NULL, NULL)) {
                                DWORD error = GetLastError();
                                LOG_ERROR("Failed to set pipe mode to message mode: {}", error);
                                log_win_error("SetNamedPipeHandleState", error);
                                
                                // 清理并抛出异常
                                CloseHandle(session_handle);
                                throw std::runtime_error("Failed to set pipe mode");
                            }
                            
                            try {
                                // Create an ASIO stream handle for this connection
                                auto pipe_stream = std::make_unique<RPCLIB_ASIO::windows::stream_handle>(io_);
                                
                                // Use safe handle assignment with the session-specific handle
                                if (!safe_handle_assign(*pipe_stream, session_handle)) {
                                    throw std::runtime_error("Failed to assign pipe handle to stream");
                                }
                                
                                // Create pipe session with the stream
                                auto session = std::make_shared<server_pipe_session<RPCLIB_ASIO::windows::stream_handle>>(
                                    parent_, &io_, std::move(*pipe_stream), parent_->disp_,
                                    suppress_exceptions_);
                                
                                // Set the session to message mode based on server configuration
                                if (use_named_pipe_message_mode_) {
                                    session->set_message_mode(true);
                                    LOG_INFO("Created pipe session with message mode enabled");
                                }
                                
                                // Start the session
                                session->start();
                                
                                // Store session reference
                                std::unique_lock<std::mutex> lock(sessions_mutex_);
                                sessions_.push_back(session);
                                win_pipe_sessions_.push_back(session);
                            } catch (const std::exception& ex) {
                                LOG_ERROR("Exception creating pipe session: {}", ex.what());
                                std::cerr << "Exception creating pipe session: " << ex.what() << std::endl;
                                
                                // Clean up if failed
                                CloseHandle(session_handle);
                            }
                            
                            // Continue accepting next connection
                            if (!this_server().stopping())
                                start_accept();
                        } else {
                            log_win_error("ConnectNamedPipe", last_error);
                            
                            // Retry
                            if (!this_server().stopping())
                                start_accept();
                        }
                    } catch (const std::exception &e) {
                        LOG_ERROR("Exception in pipe acceptance: {}", e.what());
                        std::cerr << "Pipe acceptance exception: " << e.what() << std::endl;
                        
                        // Clean up and recreate pipe
                        if (pipe_handle_ != INVALID_HANDLE_VALUE) {
                            DisconnectNamedPipe(pipe_handle_);
                            CloseHandle(pipe_handle_);
                            
                            pipe_handle_ = CreateNamedPipeA(
                                full_pipe_name_.c_str(),
                                PIPE_ACCESS_DUPLEX,
                                PIPE_TYPE_MESSAGE | PIPE_READMODE_MESSAGE,
                                PIPE_UNLIMITED_INSTANCES,
                                8192, 8192, 0, NULL);
                        }
                        
                        // Retry
                        if (!this_server().stopping())
                            io_.post([this](){ start_accept(); });
                    }
                });
#else
                // Unix Domain Socket
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
#endif
            }
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

            // Close TCP sessions
            for (auto &session : tcp_sessions_copy) {
                session->close();
            }

            // Close pipe sessions
#ifdef _WIN32
            // Handle Windows pipe sessions
            for (auto &session : win_pipe_sessions_copy) {
                session->close();
            }
#else
            // Handle Unix pipe sessions
            for (auto &session : unix_pipe_sessions_copy) {
                session->close();
            }
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
            if (it_tcp != end(tcp_sessions_)) {
                tcp_sessions_.erase(it_tcp);
            }

            auto it = std::find(begin(sessions_), end(sessions_), static_cast<std::shared_ptr<void>>(s));
            if (it != end(sessions_)) {
                sessions_.erase(it);
            }
        }

#ifdef _WIN32
        // Add Windows pipe_session closing method - specialized version
        void close_pipe_session(std::shared_ptr<server_pipe_session<RPCLIB_ASIO::windows::stream_handle>> const &s) {
            std::unique_lock<std::mutex> lock(sessions_mutex_);

            // Search directly in the type-matched container
            auto it_pipe = std::find(begin(win_pipe_sessions_), end(win_pipe_sessions_), s);
            if (it_pipe != end(win_pipe_sessions_)) {
                win_pipe_sessions_.erase(it_pipe);
            }

            // Remove from generic sessions list
            auto it = std::find(begin(sessions_), end(sessions_), static_cast<std::shared_ptr<void>>(s));
            if (it != end(sessions_)) {
                sessions_.erase(it);
            }
        }
#else
        // Unix platform - specialized version
        void close_pipe_session(std::shared_ptr<server_pipe_session<local::stream_protocol::socket>> const &s) {
            std::unique_lock<std::mutex> lock(sessions_mutex_);

            auto it_pipe = std::find(begin(unix_pipe_sessions_), end(unix_pipe_sessions_), s);
            if (it_pipe != end(unix_pipe_sessions_)) {
                unix_pipe_sessions_.erase(it_pipe);
            }

            auto it = std::find(begin(sessions_), end(sessions_), static_cast<std::shared_ptr<void>>(s));
            if (it != end(sessions_)) {
                sessions_.erase(it);
            }
        }
#endif

        unsigned short port() const {
            if (conn_type_ == conn_type::TCP) {
                return acceptor_.local_endpoint().port();
            }
            return 0;
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

        // Modified: Use specialized containers for each specific pipe type to avoid type conversion issues
#ifdef _WIN32
        std::vector<std::shared_ptr<server_pipe_session<RPCLIB_ASIO::windows::stream_handle>>> win_pipe_sessions_;
#else
        std::vector<std::shared_ptr<server_pipe_session<local::stream_protocol::socket>>> unix_pipe_sessions_;
#endif
        std::atomic_bool suppress_exceptions_;
        conn_type conn_type_;
        bool use_named_pipe_message_mode_; // New flag for message mode (NEW)
        RPCLIB_CREATE_LOG_CHANNEL(server)
        std::mutex sessions_mutex_;
    };

    RPCLIB_CREATE_LOG_CHANNEL(server)

    // Standard TCP server constructor
    server::server(uint16_t port)
            : pimpl(new server::impl(this, port)),
              disp_(std::make_shared<dispatcher>()) {
        LOG_INFO("Created server on localhost:{}", port);
        pimpl->start_accept();
    }

    server::server(server &&other) noexcept {
        *this = std::move(other);
    }

    server::server(std::string const &address, uint16_t port)
            : pimpl(new server::impl(this, address, port)),
              disp_(std::make_shared<dispatcher>()) {
        LOG_INFO("Created server on address {}:{}", address, port);
        pimpl->start_accept();
    }

    // Named Pipe server constructor - works on both Windows and Unix
    server::server(std::string const &pipe_name)
            : pimpl(new server::impl(this, pipe_name)),
              disp_(std::make_shared<dispatcher>()) {
        LOG_INFO("Created server with Named Pipe: {}", pipe_name);
        pimpl->start_accept();
    }

    // Named Pipe factory method - suitable for both Windows and Unix (MODIFIED)
    std::shared_ptr<server> server::create_named_pipe_server(const std::string &pipe_name) {
#ifdef _WIN32
        // On Windows, first create a TCP server, then convert it to Named Pipe server
        std::shared_ptr<server> srv = std::make_shared<server>(0); // Create TCP server on port 0

        // Then convert it to a Named Pipe server
        srv->pimpl->convert_to_named_pipe(pipe_name);
        
        // Set named pipe specific options (NEW)
        srv->pimpl->set_named_pipe_options();
        
        // Start accepting connections
        srv->pimpl->start_accept();

        return srv;
#else
        // On Unix use the standard constructor and configure it
        auto srv = std::make_shared<server>(pipe_name);
        // Set options if needed for Unix
        return srv;
#endif
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

    void server::close_session(std::shared_ptr<detail::server_session> const &s) {
        pimpl->close_tcp_session(s);
    }

#ifdef _WIN32
    // Windows platform specialized version
    template <>
    void server::close_pipe_session<RPCLIB_ASIO::windows::stream_handle>(
        std::shared_ptr<detail::server_pipe_session<RPCLIB_ASIO::windows::stream_handle>> const &s) {
        pimpl->close_pipe_session(s);
    }
#else
    // Unix platform specialized version
    template <>
    void server::close_pipe_session<local::stream_protocol::socket>(
            std::shared_ptr<detail::server_pipe_session<local::stream_protocol::socket>> const &s) {
        pimpl->close_pipe_session(s);
    }
#endif

} // namespace rpc
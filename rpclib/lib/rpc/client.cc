#include "rpc/client.h"
#include "rpc/config.h"
#include "rpc/rpc_error.h"

#include <iostream>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <future>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <memory>

#include "asio.hpp"
#include "format.h"

#include "rpc/detail/async_writer.h"
#include "rpc/detail/dev_utils.h"
#include "rpc/detail/response.h"
#include "rpc/detail/make_unique.h"

#ifndef _WIN32
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#else
#include <windows.h>
#endif

using namespace RPCLIB_ASIO;
using RPCLIB_ASIO::ip::tcp;
using namespace rpc::detail;

namespace rpc {
    bool enable_pipe_debug = false;
    bool enable_message_tracking = false;
    void enable_pipe_debugging(bool enable) {
        enable_pipe_debug = enable;
        enable_message_tracking = enable;
        LOG_INFO("Pipe debugging and message tracking {}", enable ? "enabled" : "disabled");
        std::cout << "Pipe debugging and message tracking " << (enable ? "enabled" : "disabled") << std::endl;
    }

    static constexpr uint32_t default_buffer_size = rpc::constants::DEFAULT_BUFFER_SIZE;
    static constexpr uint32_t message_buffer_size = 65536;

    struct client::impl {
        enum class conn_type { TCP, NAMED_PIPE };

        impl(client *parent, std::string const &addr, uint16_t port)
                : parent_(parent),
                  io_(),
                  strand_(io_),
                  call_idx_(0),
                  addr_(addr),
                  port_(port),
                  pipe_name_(""),
                  is_connected_(false),
                  state_(client::connection_state::initial),
                  conn_type_(conn_type::TCP),
                  writer_tcp_(std::make_shared<detail::async_writer<RPCLIB_ASIO::ip::tcp::socket>>( &io_, RPCLIB_ASIO::ip::tcp::socket(io_))),
                  timeout_(nonstd::nullopt),
                  connection_ec_(nonstd::nullopt),
                  work_guard_(nullptr) {
            pac_.reserve_buffer(default_buffer_size);
            work_guard_.reset(new RPCLIB_ASIO::io_service::work(io_));
        }

        impl(client *parent, std::string const &pipe_name)
                : parent_(parent),
                  io_(),
                  strand_(io_),
                  call_idx_(0),
                  addr_(""),
                  port_(0),
                  pipe_name_(pipe_name),
                  is_connected_(false),
                  state_(client::connection_state::initial),
                  conn_type_(conn_type::NAMED_PIPE),
                  timeout_(nonstd::nullopt),
                  connection_ec_(nonstd::nullopt),
                  work_guard_(nullptr) {
            pac_.reserve_buffer(default_buffer_size);
            read_buffer_.resize(message_buffer_size);
#ifdef _WIN32
            pipe_handle_ = INVALID_HANDLE_VALUE;
            full_pipe_name_ = "\\\\.\\pipe\\" + pipe_name;
            LOG_INFO("Using full pipe name: {}", full_pipe_name_);
            std::cout << "Using full pipe name: " << full_pipe_name_ << std::endl;
#else
            pipe_socket_unix_ = detail::make_unique<local::stream_protocol::socket>(io_);
#endif
            work_guard_.reset(new RPCLIB_ASIO::io_service::work(io_));
        }

        void do_connect(tcp::resolver::iterator endpoint_iterator) {
            LOG_INFO("Initiating TCP connection to {}:{}.", addr_, port_);
            connection_ec_ = nonstd::nullopt;
            RPCLIB_ASIO::async_connect(
                    writer_tcp_->socket(), endpoint_iterator,
                    strand_.wrap([this](std::error_code ec, tcp::resolver::iterator) {
                        if (!ec) {
                            std::unique_lock<std::mutex> lock(mut_connection_finished_);
                            LOG_INFO("Client connected to {}:{}", addr_, port_);
                            is_connected_ = true;
                            state_ = client::connection_state::connected;
                            conn_finished_.notify_all();
                            do_read_tcp();
                        } else {
                            std::unique_lock<std::mutex> lock(mut_connection_finished_);
                            LOG_ERROR("Error during TCP connection: {}", ec);
                            state_ = client::connection_state::disconnected;
                            connection_ec_ = ec;
                            conn_finished_.notify_all();
                        }
                    }));
        }

        // ========== Windows Named Pipe Connection ==========
        void do_connect_pipe() {
            LOG_INFO("Initiating Named Pipe connection to '{}'.", pipe_name_);
            connection_ec_ = nonstd::nullopt;
            try {
#ifdef _WIN32
                if (pipe_handle_ != INVALID_HANDLE_VALUE) {
                    LOG_INFO("Closing existing pipe handle");
                    CloseHandle(pipe_handle_);
                    pipe_handle_ = INVALID_HANDLE_VALUE;
                }

                LOG_INFO("Connecting to Windows Named Pipe: {}", full_pipe_name_);
                int max_retries = 10;
                for (int i = 0; i < max_retries; i++) {
                    LOG_INFO("Connection attempt {} of {}", i+1, max_retries);
                    // Wait for server to create the instance, otherwise CreateFile will fail
                    pipe_handle_ = CreateFileA(
                        full_pipe_name_.c_str(),
                        GENERIC_READ | GENERIC_WRITE,
                        0,
                        NULL,
                        OPEN_EXISTING,
                        FILE_FLAG_OVERLAPPED,
                        NULL);
                    if (pipe_handle_ != INVALID_HANDLE_VALUE) {
                        LOG_INFO("Connected to Windows Named Pipe on attempt {}", i + 1);
                        std::cout << "Connected to pipe on attempt " << (i + 1) << std::endl;
                        break;
                    }
                    DWORD error = GetLastError();
                    if (error == ERROR_PIPE_BUSY) {
                        LOG_INFO("Pipe busy, waiting...");
                        if (WaitNamedPipeA(full_pipe_name_.c_str(), 5000)) continue;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                }
                if (pipe_handle_ == INVALID_HANDLE_VALUE) {
                    DWORD error = GetLastError();
                    std::string err_msg = RPCLIB_FMT::format(
                        "Failed to connect to Windows Named Pipe after {} attempts: {}, error: {}",
                        max_retries, full_pipe_name_, error);
                    LOG_ERROR(err_msg);
                    std::cerr << err_msg << std::endl;
                    std::unique_lock<std::mutex> lock(mut_connection_finished_);
                    state_ = client::connection_state::disconnected;
                    connection_ec_ = std::error_code(error, std::system_category());
                    conn_finished_.notify_all();
                    throw std::runtime_error(err_msg);
                }
                DWORD mode = PIPE_READMODE_MESSAGE;
                if (!SetNamedPipeHandleState(pipe_handle_, &mode, NULL, NULL)) {
                    DWORD error = GetLastError();
                    CloseHandle(pipe_handle_);
                    pipe_handle_ = INVALID_HANDLE_VALUE;
                    std::unique_lock<std::mutex> lock(mut_connection_finished_);
                    state_ = client::connection_state::disconnected;
                    connection_ec_ = std::make_error_code(std::errc::io_error);
                    conn_finished_.notify_all();
                    throw std::runtime_error("Failed to set pipe to message mode");
                }
                try {
                    writer_pipe_win_ = std::make_shared<detail::async_writer<RPCLIB_ASIO::windows::stream_handle>>(
                        &io_, RPCLIB_ASIO::windows::stream_handle(io_, pipe_handle_));
                    writer_pipe_win_->set_message_mode(true);
                    // 交给stream_handle后，不要再 CloseHandle
                    pipe_handle_ = INVALID_HANDLE_VALUE;
                    std::unique_lock<std::mutex> lock(mut_connection_finished_);
                    LOG_INFO("Client connected to Windows Named Pipe: {}", full_pipe_name_);
                    std::cout << "Client successfully connected to pipe: " << full_pipe_name_ << std::endl;
                    is_connected_ = true;
                    state_ = client::connection_state::connected;
                    conn_finished_.notify_all();
                    do_read_pipe_win_message_framing();
                    LOG_INFO("Named Pipe client fully initialized with message mode");
                } catch (const std::exception &ex) {
                    LOG_ERROR("Error during Windows Named Pipe stream creation: {}", ex.what());
                    std::cerr << "Error during Windows Named Pipe stream creation: " << ex.what() << std::endl;
                    if (pipe_handle_ != INVALID_HANDLE_VALUE) {
                        CloseHandle(pipe_handle_);
                        pipe_handle_ = INVALID_HANDLE_VALUE;
                    }
                    std::unique_lock<std::mutex> lock(mut_connection_finished_);
                    state_ = client::connection_state::disconnected;
                    connection_ec_ = std::make_error_code(std::errc::io_error);
                    conn_finished_.notify_all();
                    throw;
                }
#else
                // Linux/Unix Domain Socket implementation remains unchanged
#endif
            } catch (const std::exception &ex) {
                LOG_ERROR("Exception during connection setup: {}", ex.what());
                std::cerr << "Exception during connection setup: " << ex.what() << std::endl;
                std::unique_lock<std::mutex> lock(mut_connection_finished_);
                state_ = client::connection_state::disconnected;
                connection_ec_ = std::make_error_code(std::errc::io_error);
                conn_finished_.notify_all();
                throw;
            }
        }

        void do_read_tcp() {
            LOG_TRACE("do_read_tcp");
            constexpr std::size_t max_read_bytes = default_buffer_size;
            writer_tcp_->socket().async_read_some(
                    RPCLIB_ASIO::buffer(pac_.buffer(), max_read_bytes),
                    [this, max_read_bytes](std::error_code ec, std::size_t length) {
                        handle_read(ec, length, max_read_bytes, [this]() { do_read_tcp(); });
                    });
        }

#ifdef _WIN32
        // Message mode framing read for Windows Named Pipe
        void do_read_pipe_win_message_framing() {
            // framing buffer for sticky-packet processing
            auto framing_buffer = std::make_shared<std::vector<char>>();
            auto read_buffer = std::make_shared<std::vector<char>>(message_buffer_size);

            // Use std::function to allow recursive lambda without copying non-copyable stream_handle
            std::function<void()> do_read;
            do_read = [this, framing_buffer, read_buffer, &do_read]() {
                if (!writer_pipe_win_) return;
                auto& stream = writer_pipe_win_->stream();
                stream.async_read_some(
                    RPCLIB_ASIO::buffer(read_buffer->data(), read_buffer->size()),
                    [this, framing_buffer, read_buffer, &do_read](std::error_code ec, std::size_t bytes_transferred) {
                        if (state_ != client::connection_state::connected) return;
                        if (ec) {
                            LOG_ERROR("Named Pipe client read error: {}", ec.message());
                            state_ = client::connection_state::disconnected;
                            return;
                        }
                        if (bytes_transferred == 0) {
                            LOG_WARN("Zero bytes read from pipe, retrying...");
                            do_read();
                            return;
                        }
                        framing_buffer->insert(framing_buffer->end(), read_buffer->data(), read_buffer->data() + bytes_transferred);
                        while (framing_buffer->size() >= 4) {
                            uint32_t msglen = 0;
                            memcpy(&msglen, framing_buffer->data(), 4);
                            if (msglen == 0 || msglen > (16 * 1024 * 1024)) {
                                LOG_ERROR("Invalid frame size: {}", msglen);
                                state_ = client::connection_state::disconnected;
                                return;
                            }
                            if (framing_buffer->size() < 4 + msglen) break;
                            pac_.reserve_buffer(msglen);
                            memcpy(pac_.buffer(), framing_buffer->data() + 4, msglen);
                            pac_.buffer_consumed(msglen);
                            RPCLIB_MSGPACK::unpacked result;
                            while (pac_.next(result)) {
                                auto r = response(std::move(result));
                                auto id = r.get_id();
                                auto it = ongoing_calls_.find(id);
                                if (it != ongoing_calls_.end()) {
                                    auto &current_call = it->second;
                                    try {
                                        if (r.get_error()) {
                                            throw rpc_error("rpc::rpc_error during call",
                                                            std::get<0>(current_call),
                                                            r.get_error());
                                        }
                                        std::get<1>(current_call)
                                                .set_value(std::move(*r.get_result()));
                                    } catch (...) {
                                        std::get<1>(current_call)
                                                .set_exception(std::current_exception());
                                    }
                                    strand_.post(
                                            [this, id]() { ongoing_calls_.erase(id); });
                                } else {
                                    LOG_WARN("Received response for unknown call id: {}", id);
                                }
                            }
                            framing_buffer->erase(framing_buffer->begin(), framing_buffer->begin() + 4 + msglen);
                        }
                        do_read();
                    }
                );
            };
            do_read();
        }
#endif

        template<typename ReadAgainFunc>
        void handle_read(std::error_code ec, std::size_t length, std::size_t max_read_bytes, ReadAgainFunc read_again) {
            if (!ec) {
                LOG_TRACE("Read chunk of size {}", length);
                pac_.buffer_consumed(length);
                RPCLIB_MSGPACK::unpacked result;
                while (pac_.next(result)) {
                    auto r = response(std::move(result));
                    auto id = r.get_id();
                    auto it = ongoing_calls_.find(id);
                    if (it != ongoing_calls_.end()) {
                        auto &current_call = it->second;
                        try {
                            if (r.get_error()) {
                                throw rpc_error("rpc::rpc_error during call",
                                                std::get<0>(current_call),
                                                r.get_error());
                            }
                            std::get<1>(current_call)
                                    .set_value(std::move(*r.get_result()));
                        } catch (...) {
                            std::get<1>(current_call)
                                    .set_exception(std::current_exception());
                        }
                        strand_.post(
                                [this, id]() { ongoing_calls_.erase(id); });
                    } else {
                        LOG_WARN("Received response for unknown call id: {}", id);
                    }
                }
                if (pac_.buffer_capacity() < max_read_bytes) {
                    LOG_TRACE("Reserving extra buffer: {}", max_read_bytes);
                    pac_.reserve_buffer(max_read_bytes);
                }
                read_again();
            } else if (ec == RPCLIB_ASIO::error::eof) {
                LOG_WARN("The server closed the connection.");
                state_ = client::connection_state::disconnected;
            } else if (ec == RPCLIB_ASIO::error::connection_reset) {
                state_ = client::connection_state::disconnected;
                LOG_WARN("The connection was reset.");
            } else {
                LOG_ERROR("Unhandled error code: {} | '{}'", ec, ec.message());
            }
        }

        client::connection_state get_connection_state() const { return state_; }
        client::connection_type get_connection_type() const {
            return conn_type_ == conn_type::TCP ?
                   client::connection_type::TCP :
                   client::connection_type::NAMED_PIPE;
        }
        void write(RPCLIB_MSGPACK::sbuffer item) {
            std::cout << "[pimpl->write] called, conn_type_=" << static_cast<int>(conn_type_) << std::endl;
            if (conn_type_ == conn_type::TCP) {
                std::cout << "[pimpl->write] using writer_tcp_ to write, sbuffer size=" << item.size() << std::endl;
                if (writer_tcp_) {
                    writer_tcp_->write(std::move(item));
                } else {
                    std::cout << "[pimpl->write] writer_tcp_ is nullptr!" << std::endl;
                }
            }
        #ifdef _WIN32
            else if (writer_pipe_win_) {
                std::cout << "[pimpl->write] using writer_pipe_win_ to write, sbuffer size=" << item.size() << std::endl;
                writer_pipe_win_->write(std::move(item));
            }
            else {
                std::cout << "[pimpl->write] writer_pipe_win_ is nullptr!" << std::endl;
                LOG_ERROR("No valid writer available for write operation (writer_pipe_win_ is nullptr)");
                std::cerr << "No valid writer available for write operation (writer_pipe_win_ is nullptr)" << std::endl;
            }
        #else
            else {
                LOG_ERROR("No valid writer available for write operation");
                std::cerr << "No valid writer available for write operation" << std::endl;
            }
        #endif
        }
        nonstd::optional<int64_t> get_timeout() { return timeout_; }
        void set_timeout(int64_t value) { timeout_ = value; }
        void clear_timeout() { timeout_ = nonstd::nullopt; }
        using call_t = std::pair<std::string, std::promise<RPCLIB_MSGPACK::object_handle>>;
        client *parent_;
        RPCLIB_ASIO::io_service io_;
        RPCLIB_ASIO::strand strand_;
        std::atomic<int> call_idx_;
        std::unordered_map<uint32_t, call_t> ongoing_calls_;
        std::string addr_;
        uint16_t port_;
        std::string pipe_name_;
        RPCLIB_MSGPACK::unpacker pac_;
        std::vector<char> read_buffer_;
        std::atomic_bool is_connected_;
        std::condition_variable conn_finished_;
        std::mutex mut_connection_finished_;
        std::thread io_thread_;
        std::atomic<client::connection_state> state_;
        conn_type conn_type_;
#ifdef _WIN32
        HANDLE pipe_handle_;
        std::string full_pipe_name_;
        std::shared_ptr<detail::async_writer<RPCLIB_ASIO::windows::stream_handle>> writer_pipe_win_;
#else
        std::unique_ptr<local::stream_protocol::socket> pipe_socket_unix_;
        std::shared_ptr<detail::async_writer<local::stream_protocol::socket>> writer_pipe_unix_;
#endif
        std::shared_ptr<detail::async_writer<RPCLIB_ASIO::ip::tcp::socket>> writer_tcp_;
        nonstd::optional<int64_t> timeout_;
        nonstd::optional<std::error_code> connection_ec_;
        RPCLIB_CREATE_LOG_CHANNEL(client)
        std::unique_ptr<RPCLIB_ASIO::io_service::work> work_guard_;
    };

    client::client(std::string const &addr, uint16_t port)
            : pimpl(new client::impl(this, addr, port)) {
        tcp::resolver resolver(pimpl->io_);
        auto endpoint_it =
                resolver.resolve({pimpl->addr_, std::to_string(pimpl->port_)});
        pimpl->do_connect(endpoint_it);
        std::thread io_thread([this]() {
            RPCLIB_CREATE_LOG_CHANNEL(client)
            name_thread("client");
            pimpl->io_.run();
        });
        pimpl->io_thread_ = std::move(io_thread);
    }

    client::client(std::string const &pipe_name)
        : pimpl(new client::impl(this, pipe_name)) {
    set_timeout(15000);

    std::thread io_thread([this]() {
        try {
            RPCLIB_CREATE_LOG_CHANNEL(client)
            name_thread("client_pipe");
            pimpl->io_.run();
        } catch (const std::exception &ex) {
            LOG_ERROR("Exception in io_thread: {}", ex.what());
            std::cerr << "IO thread exception: " << ex.what() << std::endl;
        }
    });
    pimpl->io_thread_ = std::move(io_thread);

#ifndef _WIN32
    struct stat buffer;
    bool file_exists = (stat(pipe_name.c_str(), &buffer) == 0);
    if (!file_exists) {
        std::string err_msg = RPCLIB_FMT::format(
                "Unix Domain Socket file does not exist: {}", pipe_name);
        LOG_ERROR(err_msg);
        throw std::runtime_error(err_msg);
    } else if (!S_ISSOCK(buffer.st_mode)) {
        std::string err_msg = RPCLIB_FMT::format(
                "Path exists but is not a valid socket: {}", pipe_name);
        LOG_ERROR(err_msg);
        throw std::runtime_error(err_msg);
    }
#endif

    try {
        pimpl->do_connect_pipe();
        try {
            LOG_INFO("Trying initial connection...");
            wait_conn();
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        } catch (const rpc::timeout &ex) {
            LOG_WARN("Initial connection timed out, will retry later: {}", ex.what());
            std::cerr << "Initial connection timed out: " << ex.what() << std::endl;
        } catch (const std::runtime_error &ex) {
            LOG_ERROR("Critical connection error: {}", ex.what());
            std::cerr << "Critical connection error: " << ex.what() << std::endl;
            throw;
        } catch (const std::exception &ex) {
            LOG_WARN("Initial connection failed: {}, will retry later", ex.what());
            std::cerr << "Initial connection failed: " << ex.what() << std::endl;
        }
    } catch (const std::exception &ex) {
        LOG_ERROR("Failed to initialize client: {}", ex.what());
        std::cerr << "Failed to initialize client: " << ex.what() << std::endl;
        throw;
    }
}

    void client::wait_conn() {
        std::unique_lock<std::mutex> lock(pimpl->mut_connection_finished_);
        if (pimpl->is_connected_) return;
        const auto max_wait_time = std::chrono::seconds(20);
        auto wait_end_time = std::chrono::steady_clock::now() + max_wait_time;
        int retry_count = 0;
        const int max_retries = 5;
        while (!pimpl->is_connected_ && retry_count < max_retries) {
            if (auto ec = pimpl->connection_ec_) {
                LOG_ERROR("Connection error: {} ({})", ec->value(), ec->message());
                std::cerr << "Connection error: " << ec->message() << std::endl;
#ifndef _WIN32
                if (pimpl->conn_type_ == client::impl::conn_type::NAMED_PIPE) {
                    if (ec->value() == static_cast<int>(std::errc::connection_refused) ||
                        ec->value() == static_cast<int>(std::errc::no_such_file_or_directory)) {
                        LOG_WARN("Socket connection refused or file not found, server may not be ready");
                        pimpl->connection_ec_ = nonstd::nullopt;
                        retry_count++;
                        lock.unlock();
                        std::this_thread::sleep_for(std::chrono::milliseconds(500));
                        lock.lock();
                        if (pimpl->pipe_socket_unix_) {
                            lock.unlock();
                            pimpl->do_connect_pipe();
                            lock.lock();
                        }
                        continue;
                    }
                }
#endif
                throw rpc::system_error(*ec);
            }
            if (auto timeout = pimpl->timeout_) {
                LOG_INFO("Waiting for connection with timeout {}ms (retry {}/{})",
                          *timeout, retry_count + 1, max_retries);
                std::cout << "Waiting for connection with timeout " << *timeout << "ms (retry " 
                          << (retry_count + 1) << "/" << max_retries << ")" << std::endl;
                auto result = pimpl->conn_finished_.wait_for(
                        lock, std::chrono::milliseconds(*timeout));
                if (result == std::cv_status::timeout) {
                    retry_count++;
                    if (retry_count >= max_retries) {
                        std::string err_msg = RPCLIB_FMT::format(
                                "Timeout of {}ms while connecting to named pipe '{}' (after {} retries)",
                                *timeout, pimpl->pipe_name_, retry_count);
                        LOG_ERROR(err_msg);
                        std::cerr << err_msg << std::endl;
                        throw rpc::timeout(err_msg);
                    }
                    LOG_WARN("Connection attempt {} timed out, retrying...", retry_count);
                    std::cout << "Connection attempt " << retry_count << " timed out, retrying..." << std::endl;
                    if (pimpl->conn_type_ == client::impl::conn_type::NAMED_PIPE) {
                        lock.unlock();
                        pimpl->do_connect_pipe();
                        lock.lock();
                    }
                    continue;
                }
            } else {
                auto now = std::chrono::steady_clock::now();
                if (now >= wait_end_time) {
                    std::string err_msg = "Connection timed out after maximum wait period";
                    LOG_ERROR(err_msg);
                    std::cerr << err_msg << std::endl;
                    throw rpc::timeout(err_msg);
                }
                auto wait_status = pimpl->conn_finished_.wait_for(lock, std::chrono::seconds(1));
                if (wait_status == std::cv_status::timeout) {
                    LOG_WARN("Still waiting for connection... (retry {}/{})", retry_count + 1, max_retries);
                    std::cout << "Still waiting for connection... (retry " << (retry_count + 1)  
                              << "/" << max_retries << ")" << std::endl;
                    retry_count++;
                    if (retry_count < max_retries && pimpl->conn_type_ == client::impl::conn_type::NAMED_PIPE) {
                        lock.unlock();
                        pimpl->do_connect_pipe();
                        lock.lock();
                    }
                }
            }
            if (pimpl->is_connected_) {
                LOG_INFO("Connected successfully");
                std::cout << "Connected successfully" << std::endl;
                return;
            }
        }
        if (!pimpl->is_connected_) {
            std::string err_msg = "Failed to connect after multiple attempts";
            LOG_ERROR(err_msg);
            std::cerr << err_msg << std::endl;
            throw rpc::timeout(err_msg);
        }
    }

    int client::get_next_call_idx() { return ++(pimpl->call_idx_); }
    void client::post(std::shared_ptr<RPCLIB_MSGPACK::sbuffer> buffer, int idx,
                  std::string const &func_name,
                  std::shared_ptr<rsp_promise> p) {
    std::cout << "[client::post] called (with idx, func_name='" << func_name << "')" << std::endl;
    pimpl->strand_.post([=]() {
        std::cout << "[client::post/strand] lambda running for func_name='" << func_name << "', idx=" << idx << std::endl;
        if (enable_message_tracking || enable_pipe_debug) {
            LOG_INFO("Posting RPC call '{}' with ID {}", func_name, idx);
            std::cout << "Posting RPC call '" << func_name << "' with ID " << idx << std::endl;
        }
        pimpl->ongoing_calls_.insert(
                std::make_pair(idx, std::make_pair(func_name, std::move(*p))));
        std::cout << "[client::post/strand] calling pimpl->write(), buffer size=" << (buffer ? buffer->size() : 0) << std::endl;
        pimpl->write(std::move(*buffer));
    });
}

void client::post(RPCLIB_MSGPACK::sbuffer *buffer) {
    std::cout << "[client::post] called (notification/one-way), buffer ptr=" << buffer << std::endl;
    pimpl->strand_.post([=]() {
        std::cout << "[client::post/strand] notification lambda running, buffer ptr=" << buffer << std::endl;
        if (buffer) {
            std::cout << "[client::post/strand] calling pimpl->write(), buffer size=" << buffer->size() << std::endl;
            pimpl->write(std::move(*buffer));
            delete buffer;
        } else {
            std::cout << "[client::post/strand] buffer is nullptr!" << std::endl;
        }
    });
}
    client::connection_state client::get_connection_state() const { return pimpl->get_connection_state(); }
    client::connection_type client::get_connection_type() const { return pimpl->get_connection_type(); }
    nonstd::optional<int64_t> client::get_timeout() const { return pimpl->get_timeout(); }
    void client::set_timeout(int64_t value) { pimpl->set_timeout(value); }
    void client::clear_timeout() { pimpl->clear_timeout(); }
    void client::wait_all_responses() {
        for (auto &c : pimpl->ongoing_calls_) {
            c.second.second.get_future().wait();
        }
    }
    RPCLIB_NORETURN void client::throw_timeout(std::string const &func_name) {
        throw rpc::timeout(
                RPCLIB_FMT::format("Timeout of {}ms while calling RPC function '{}'",
                                   *get_timeout(), func_name));
    }
    client::~client() {
        pimpl->work_guard_.reset();
        pimpl->io_.stop();
        pimpl->io_thread_.join();
#ifdef _WIN32
        pimpl->pipe_handle_ = INVALID_HANDLE_VALUE;
#endif
    }
} // namespace rpc
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

#include "asio.hpp"
#include "format.h"

#include "rpc/detail/async_writer.h"
#include "rpc/detail/dev_utils.h"
#include "rpc/detail/response.h"
#include "rpc/detail/make_unique.h"
#include "rpc/detail/pipe_utils.h" 

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

    static constexpr uint32_t default_buffer_size =
    rpc::constants::DEFAULT_BUFFER_SIZE;

    // Larger buffer size for message mode
    static constexpr uint32_t message_buffer_size = 65536;

    struct client::impl {
        // Connection type enum - for internal use
        enum class conn_type { TCP, NAMED_PIPE };

        // TCP constructor
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
                  writer_tcp_(std::make_shared<detail::async_writer<RPCLIB_ASIO::ip::tcp::socket>>(
                          &io_, RPCLIB_ASIO::ip::tcp::socket(io_))),
                  timeout_(nonstd::nullopt),
                  connection_ec_(nonstd::nullopt) {
            pac_.reserve_buffer(default_buffer_size);
        }

        // Named Pipe constructor - updated for Windows support
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
                  connection_ec_(nonstd::nullopt) {
            pac_.reserve_buffer(default_buffer_size);
            // Initialize read buffer for message mode with larger size
            read_buffer_.resize(message_buffer_size); // Increased buffer size

#ifdef _WIN32
            // Initialize pipe handle as invalid in Windows platform
            pipe_handle_ = INVALID_HANDLE_VALUE;
            full_pipe_name_ = "\\\\.\\pipe\\" + pipe_name;
            LOG_INFO("Using full pipe name: {}", full_pipe_name_);
            std::cout << "Using full pipe name: " << full_pipe_name_ << std::endl;
#else
            // Unix/Linux platform - create socket for Unix Domain Socket
            pipe_socket_unix_ = detail::make_unique<local::stream_protocol::socket>(io_);
#endif
        }

        // TCP connection function
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

        // Named Pipe connection function - updated for Windows message mode support
        void do_connect_pipe() {
            LOG_INFO("Initiating Named Pipe connection to '{}'.", pipe_name_);
            connection_ec_ = nonstd::nullopt;
            try {
#ifdef _WIN32
                // Windows platform - Connect to Windows Named Pipe
                // Use Windows API directly to create named pipe connection
                if (pipe_handle_ != INVALID_HANDLE_VALUE) {
                    LOG_INFO("Closing existing pipe handle");
                    CloseHandle(pipe_handle_);
                    pipe_handle_ = INVALID_HANDLE_VALUE;
                }
        
                LOG_INFO("Connecting to Windows Named Pipe: {}", full_pipe_name_);
                
                // Try to connect with retries for busy pipes - increased max retries
                int max_retries = 10;
                for (int i = 0; i < max_retries; i++) {
                    LOG_INFO("Connection attempt {} of {}", i+1, max_retries);
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
                    log_win_error("CreateFile for pipe", error);
                    
                    if (error == ERROR_PIPE_BUSY) {
                        LOG_INFO("Pipe busy, waiting...");
                        if (WaitNamedPipeA(full_pipe_name_.c_str(), 5000)) {
                            // Pipe is now available, retry connection
                            continue;
                        }
                        log_win_error("WaitNamedPipe", GetLastError());
                    }
                    
                    // Short delay before retrying
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
        
                // Set pipe to message mode - CRITICAL FOR PROPER OPERATION
                DWORD mode = PIPE_READMODE_MESSAGE;
                if (!SetNamedPipeHandleState(pipe_handle_, &mode, NULL, NULL)) {
                    DWORD error = GetLastError();
                    log_win_error("SetNamedPipeHandleState", error);
                    CloseHandle(pipe_handle_);
                    pipe_handle_ = INVALID_HANDLE_VALUE;
                    
                    std::unique_lock<std::mutex> lock(mut_connection_finished_);
                    state_ = client::connection_state::disconnected;
                    connection_ec_ = std::error_code(error, std::system_category());
                    conn_finished_.notify_all();
                    throw std::runtime_error("Failed to set pipe to message mode");
                }
        
                // Pipe connection succeeded
                try {
                    // 保存原始句柄用于写入
                    HANDLE write_handle = pipe_handle_;
                    HANDLE read_handle = INVALID_HANDLE_VALUE;
                    
                    LOG_INFO("Duplicating pipe handle for separate read/write operations");
                    // 复制句柄用于读取操作
                    if (!DuplicateHandle(
                            GetCurrentProcess(),  // 源进程
                            write_handle,         // 源句柄
                            GetCurrentProcess(),  // 目标进程
                            &read_handle,         // 目标句柄引用
                            0,                    // 与源相同的访问权限
                            FALSE,                // 不继承
                            DUPLICATE_SAME_ACCESS)) { // 相同访问权限
                        
                        DWORD error = GetLastError();
                        log_win_error("DuplicateHandle", error);
                        throw std::runtime_error("Failed to duplicate pipe handle for reading");
                    }
                    
                    LOG_INFO("Successfully duplicated handle - Write: {}, Read: {}", 
                             (void*)write_handle, (void*)read_handle);
                    
                    // 先为写入操作创建 stream_handle
                    LOG_INFO("Creating pipe stream for writing");
                    pipe_stream_ = std::make_shared<RPCLIB_ASIO::windows::stream_handle>(io_);
                    
                    // 为写流分配写入句柄
                    if (!safe_handle_assign(*pipe_stream_, write_handle)) {
                        // 如果分配失败，释放读取句柄
                        CloseHandle(read_handle);
                        throw std::runtime_error("Failed to assign pipe handle to write stream");
                    }
                    
                    // 创建写入器
                    LOG_INFO("Creating async writer");
                    writer_pipe_win_ = std::make_shared<detail::async_writer<RPCLIB_ASIO::windows::stream_handle>>(
                        &io_, std::move(*pipe_stream_));
                    writer_pipe_win_->set_message_mode(true); // Enable message mode
                    
                    // 为读取操作创建新的 stream_handle
                    LOG_INFO("Creating pipe stream for reading");
                    pipe_stream_ = std::make_shared<RPCLIB_ASIO::windows::stream_handle>(io_);
                    
                    // 为读流分配读取句柄
                    if (!safe_handle_assign(*pipe_stream_, read_handle)) {
                        throw std::runtime_error("Failed to assign pipe handle to read stream");
                    }
        
                    // Only set state flags on successful connection
                    {
                        std::unique_lock<std::mutex> lock(mut_connection_finished_);
                        LOG_INFO("Client connected to Windows Named Pipe: {}", full_pipe_name_);
                        std::cout << "Client successfully connected to pipe: " << full_pipe_name_ << std::endl;
                        is_connected_ = true;
                        state_ = client::connection_state::connected;
                        conn_finished_.notify_all();
                    }
        
                    // Start reading data
                    LOG_INFO("Starting message-mode read operation");
                    do_read_pipe_win_message();
                    
                    LOG_INFO("Named Pipe client fully initialized with message mode");
                } catch (const std::exception &ex) {
                    // Handle stream creation errors
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
                // Unix/Linux platform - Connect to Unix Domain Socket

                // Ensure pipe_socket_unix_ is properly initialized
                if (!pipe_socket_unix_) {
                    pipe_socket_unix_ = detail::make_unique<local::stream_protocol::socket>(io_);
                }

                // Check if socket file exists and is valid
                struct stat buffer;
                bool file_exists = (stat(pipe_name_.c_str(), &buffer) == 0);

                if (!file_exists) {
                    // File doesn't exist, throw exception
                    std::string err_msg = RPCLIB_FMT::format(
                            "Unix Domain Socket file does not exist: {}", pipe_name_);
                    LOG_ERROR(err_msg);
                    throw std::runtime_error(err_msg);
                } else if (!S_ISSOCK(buffer.st_mode)) {
                    // File exists but is not a socket, throw exception
                    std::string err_msg = RPCLIB_FMT::format(
                            "Path exists but is not a valid socket: {}", pipe_name_);
                    LOG_ERROR(err_msg);
                    throw std::runtime_error(err_msg);
                } else {
                    LOG_INFO("Found valid socket file: {}", pipe_name_);

                    // File check passed, try to connect
                    pipe_socket_unix_->async_connect(
                            local::stream_protocol::endpoint(pipe_name_),
                            strand_.wrap([this](std::error_code ec) {
                                if (!ec) {
                                    std::unique_lock<std::mutex> lock(mut_connection_finished_);
                                    LOG_INFO("Client connected to Unix Domain Socket: {}", pipe_name_);

                                    try {
                                        // Create templated writer instance
                                        writer_pipe_unix_ = std::make_shared<detail::async_writer<local::stream_protocol::socket>>(
                                                &io_, std::move(*pipe_socket_unix_));

                                        // Only set state flags on successful connection
                                        is_connected_ = true;
                                        state_ = client::connection_state::connected;
                                        conn_finished_.notify_all();

                                        do_read_pipe_unix();
                                    } catch (const std::exception &ex) {
                                        LOG_ERROR("Error during writer creation: {}", ex.what());
                                        connection_ec_ = std::make_error_code(std::errc::io_error);
                                        state_ = client::connection_state::disconnected;
                                        conn_finished_.notify_all();
                                    }
                                } else {
                                    std::unique_lock<std::mutex> lock(mut_connection_finished_);
                                    LOG_ERROR("Error during Unix Domain Socket connection: {} ({})", ec.value(), ec.message());
                                    state_ = client::connection_state::disconnected;
                                    connection_ec_ = ec;
                                    conn_finished_.notify_all();
                                }
                            }));
                }
#endif
            } catch (const std::exception &ex) {
                LOG_ERROR("Exception during connection setup: {}", ex.what());
                std::cerr << "Exception during connection setup: " << ex.what() << std::endl;
                std::unique_lock<std::mutex> lock(mut_connection_finished_);
                state_ = client::connection_state::disconnected;
                connection_ec_ = std::make_error_code(std::errc::io_error);
                conn_finished_.notify_all();
                // Rethrow exception to ensure it's propagated to caller
                throw;
            }
        }

        // TCP read function
        void do_read_tcp() {
            LOG_TRACE("do_read_tcp");
            constexpr std::size_t max_read_bytes = default_buffer_size;

            // Use TCP-specific writer
            writer_tcp_->socket().async_read_some(
                    RPCLIB_ASIO::buffer(pac_.buffer(), max_read_bytes),
                    [this, max_read_bytes](std::error_code ec, std::size_t length) {
                        handle_read(ec, length, max_read_bytes, [this](){ do_read_tcp(); });
                    });
        }

#ifdef _WIN32
        // Windows Named Pipe message mode read function - ENHANCED IMPLEMENTATION
        void do_read_pipe_win_message() {
            LOG_INFO("Reading from Windows Named Pipe in message mode");
            
            // Ensure pipe_stream_ exists and client is connected
            if (!pipe_stream_ || !is_connected_) {
                LOG_ERROR("Pipe stream is null or client disconnected in message mode read");
                std::cerr << "Pipe stream is null or client disconnected in message mode read" << std::endl;
                return;
            }

            // Use pipe_stream_ to read messages with larger buffer
            pipe_stream_->async_read_some(
                RPCLIB_ASIO::buffer(read_buffer_.data(), read_buffer_.size()),
                strand_.wrap([this](std::error_code ec, std::size_t bytes_read) {
                    if (ec) {
                        // Handle error cases
                        if (ec == RPCLIB_ASIO::error::eof || ec == RPCLIB_ASIO::error::connection_reset) {
                            LOG_WARN("Pipe connection closed by server");
                            std::cerr << "Pipe connection closed by server" << std::endl;
                            state_ = client::connection_state::disconnected;
                        } else {
                            LOG_ERROR("Error reading from pipe: {} ({})", ec.value(), ec.message());
                            std::cerr << "Error reading from pipe: " << ec.message() << std::endl;
                        }
                        return;
                    }

                    if (bytes_read == 0) {
                        LOG_WARN("Read zero bytes from pipe, retrying");
                        std::cerr << "Read zero bytes from pipe, retrying" << std::endl;
                        do_read_pipe_win_message();
                        return;
                    }

                    LOG_INFO("Read {} bytes from pipe in message mode", bytes_read);
                    
                    // Enhanced debugging - log what we received
                    if (enable_message_tracking || enable_pipe_debug) {
                        std::string hex_dump;
                        for (size_t i = 0; i < std::min(bytes_read, size_t(64)); ++i) {
                            char buf[8];
                            snprintf(buf, sizeof(buf), "%02x ", (unsigned char)read_buffer_[i]);
                            hex_dump += buf;
                        }
                        LOG_INFO("Message content (first 64 bytes): {}", hex_dump);
                        std::cout << "Received message (first 64 bytes): " << hex_dump << std::endl;
                    }
                    
                    try {
                        // Process complete message directly
                        LOG_INFO("Unpacking message...");
                        RPCLIB_MSGPACK::unpacked result;
                        RPCLIB_MSGPACK::unpack(result, read_buffer_.data(), bytes_read);
                        
                        // Create response object and process it
                        LOG_INFO("Creating response object");
                        auto r = response(std::move(result));
                        auto id = r.get_id();
                        
                        LOG_INFO("Response for call ID: {}", id);
                        
                        // Find matching call
                        auto call_iter = ongoing_calls_.find(id);
                        if (call_iter != ongoing_calls_.end()) {
                            LOG_INFO("Found matching call with id: {}", id);
                            std::cout << "Found matching call with ID: " << id << std::endl;
                            auto &current_call = call_iter->second;
                            
                            try {
                                if (r.get_error()) {
                                    LOG_WARN("RPC call returned error");
                                    throw rpc_error("rpc::rpc_error during call",
                                                std::get<0>(current_call),
                                                r.get_error());
                                } else {
                                    LOG_INFO("Setting successful result");
                                    std::get<1>(current_call)
                                        .set_value(std::move(*r.get_result()));
                                }
                            } catch (const std::exception& e) {
                                LOG_ERROR("Exception while setting result: {}", e.what());
                                std::cerr << "Exception while setting result: " << e.what() << std::endl;
                                std::get<1>(current_call)
                                    .set_exception(std::current_exception());
                            }
                            
                            // Remove call from map
                            strand_.post([this, id]() { 
                                ongoing_calls_.erase(id); 
                                LOG_INFO("Removed call ID {} from ongoing calls", id);
                            });
                        } else {
                            LOG_WARN("Received response for unknown call id: {}", id);
                            std::cerr << "Received response for unknown call id: " << id << std::endl;
                        }
                    }
                    catch (const std::exception& e) {
                        LOG_ERROR("Exception while processing message: {}", e.what());
                        std::cerr << "Message processing error: " << e.what() << std::endl;
                    }
                    
                    // Clear buffer for next read
                    std::fill(read_buffer_.begin(), read_buffer_.end(), 0);
                    
                    // Continue reading
                    do_read_pipe_win_message();
                }));
        }

        // Legacy Windows pipe read function - kept for compatibility
        void do_read_pipe_win() {
            LOG_TRACE("do_read_pipe_win");
            constexpr std::size_t max_read_bytes = default_buffer_size;

            // Ensure pipe_stream_ exists
            if (!pipe_stream_) {
                LOG_ERROR("Pipe stream is null in do_read_pipe_win");
                return;
            }

            // Use Windows Named Pipe specific stream_handle
            pipe_stream_->async_read_some(
                RPCLIB_ASIO::buffer(pac_.buffer(), max_read_bytes),
                [this, max_read_bytes](std::error_code ec, std::size_t length) {
                    handle_read(ec, length, max_read_bytes, [this](){ do_read_pipe_win(); });
                });
        }
#else
        // Unix Domain Socket read function
        void do_read_pipe_unix() {
            LOG_TRACE("do_read_pipe_unix");
            constexpr std::size_t max_read_bytes = default_buffer_size;

            // Use Unix Domain Socket specific writer
            writer_pipe_unix_->socket().async_read_some(
                    RPCLIB_ASIO::buffer(pac_.buffer(), max_read_bytes),
                    [this, max_read_bytes](std::error_code ec, std::size_t length) {
                        handle_read(ec, length, max_read_bytes, [this](){ do_read_pipe_unix(); });
                    });
        }
#endif

        // Generic read handler function
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

                // resizing strategy: if the remaining buffer size is
                // less than the maximum bytes requested from asio,
                // then request max_read_bytes. This prompts the unpacker
                // to resize its buffer doubling its size
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
                LOG_ERROR("Unhandled error code: {} | '{}'", ec,
                          ec.message());
            }
        }

        client::connection_state get_connection_state() const { return state_; }

        // Get connection type
        client::connection_type get_connection_type() const {
            return conn_type_ == conn_type::TCP ?
                   client::connection_type::TCP :
                   client::connection_type::NAMED_PIPE;
        }

        // Write function that selects writer based on connection type - updated for Windows
        void write(RPCLIB_MSGPACK::sbuffer item) {
            if (conn_type_ == conn_type::TCP) {
                writer_tcp_->write(std::move(item));
            }
#ifdef _WIN32
            else if (writer_pipe_win_) {
                // Debug the message being sent
                if (enable_message_tracking || enable_pipe_debug) {
                    std::string hex_dump;
                    for (size_t i = 0; i < std::min(item.size(), size_t(64)); ++i) {
                        char buf[8];
                        snprintf(buf, sizeof(buf), "%02x ", (unsigned char)item.data()[i]);
                        hex_dump += buf;
                    }
                    LOG_INFO("Sending message (first 64 bytes): {}", hex_dump);
                    std::cout << "Sending message (first 64 bytes): " << hex_dump << std::endl;
                }
                
                // Use Windows pipe writer with message mode
                writer_pipe_win_->write(std::move(item));
            }
#else
            else if (writer_pipe_unix_) {
                writer_pipe_unix_->write(std::move(item));
            }
#endif
            else {
                LOG_ERROR("No valid writer available for write operation");
                std::cerr << "No valid writer available for write operation" << std::endl;
            }
        }

        nonstd::optional<int64_t> get_timeout() { return timeout_; }

        void set_timeout(int64_t value) { timeout_ = value; }

        void clear_timeout() { timeout_ = nonstd::nullopt; }

        using call_t =
                std::pair<std::string, std::promise<RPCLIB_MSGPACK::object_handle>>;

        client *parent_;
        RPCLIB_ASIO::io_service io_;
        RPCLIB_ASIO::strand strand_;
        std::atomic<int> call_idx_; /// The index of the last call made
        std::unordered_map<uint32_t, call_t> ongoing_calls_;

        // Connection information
        std::string addr_;
        uint16_t port_;
        std::string pipe_name_;

        RPCLIB_MSGPACK::unpacker pac_;
        std::vector<char> read_buffer_; // Buffer for message mode reading
        std::atomic_bool is_connected_;
        std::condition_variable conn_finished_;
        std::mutex mut_connection_finished_;
        std::thread io_thread_;
        std::atomic<client::connection_state> state_;

        // Connection type
        conn_type conn_type_;

        // Named Pipe related members - updated for Windows message mode
#ifdef _WIN32
        HANDLE pipe_handle_;  // Windows Named Pipe handle
        std::string full_pipe_name_;  // Complete Named Pipe path
        std::shared_ptr<RPCLIB_ASIO::windows::stream_handle> pipe_stream_;  // Stream handle
        std::shared_ptr<detail::async_writer<RPCLIB_ASIO::windows::stream_handle>> writer_pipe_win_; // Windows pipe writer
#else
        std::unique_ptr<local::stream_protocol::socket> pipe_socket_unix_;
        std::shared_ptr<detail::async_writer<local::stream_protocol::socket>> writer_pipe_unix_;
#endif

        // TCP related members
        std::shared_ptr<detail::async_writer<RPCLIB_ASIO::ip::tcp::socket>> writer_tcp_;

        nonstd::optional<int64_t> timeout_;
        nonstd::optional<std::error_code> connection_ec_;
        RPCLIB_CREATE_LOG_CHANNEL(client)
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
        try {
            // Set default timeout to avoid waiting forever
            set_timeout(15000); // Increased default timeout to 15 seconds

            // Check and handle Socket file on Linux
#ifndef _WIN32
            // Check if file exists and is a valid socket before any connection attempts
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

            // File check passed, start connection
            pimpl->do_connect_pipe();
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

            try {
                LOG_INFO("Trying initial connection...");
                wait_conn();  // Try to connect immediately
                
                // Short delay to ensure connection is stable
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            } catch (const rpc::timeout &ex) {
                LOG_WARN("Initial connection timed out, will retry later: {}", ex.what());
                std::cerr << "Initial connection timed out: " << ex.what() << std::endl;
                // Timeout can be ignored, subsequent calls will retry
            } catch (const std::runtime_error &ex) {
                // For file not found or invalid errors, we don't want to retry
                LOG_ERROR("Critical connection error: {}", ex.what());
                std::cerr << "Critical connection error: " << ex.what() << std::endl;
                throw; // Rethrow to ensure client initialization fails
            } catch (const std::exception &ex) {
                LOG_WARN("Initial connection failed: {}, will retry later", ex.what());
                std::cerr << "Initial connection failed: " << ex.what() << std::endl;
                // Other exceptions can also be ignored, subsequent calls will retry
            }

        } catch (const std::exception &ex) {
            LOG_ERROR("Failed to initialize client: {}", ex.what());
            std::cerr << "Failed to initialize client: " << ex.what() << std::endl;
            throw;
        }
    }

    void client::wait_conn() {
        std::unique_lock<std::mutex> lock(pimpl->mut_connection_finished_);

        // If already connected, return immediately
        if (pimpl->is_connected_) {
            return;
        }

        // Add timeout protection to avoid blocking forever
        const auto max_wait_time = std::chrono::seconds(20); // Increased max wait time
        auto wait_end_time = std::chrono::steady_clock::now() + max_wait_time;

        int retry_count = 0;
        const int max_retries = 5; // Increased max retry count

        while (!pimpl->is_connected_ && retry_count < max_retries) {
            // Check if there's already an error
            if (auto ec = pimpl->connection_ec_) {
                LOG_ERROR("Connection error: {} ({})", ec->value(), ec->message());
                std::cerr << "Connection error: " << ec->message() << std::endl;

#ifndef _WIN32
                // For Unix Domain Socket, try to handle specific errors
                if (pimpl->conn_type_ == client::impl::conn_type::NAMED_PIPE) {

                    if (ec->value() == static_cast<int>(std::errc::connection_refused) ||
                        ec->value() == static_cast<int>(std::errc::no_such_file_or_directory)) {
                        LOG_WARN("Socket connection refused or file not found, server may not be ready");
                        // Continue trying, don't throw exception
                        pimpl->connection_ec_ = nonstd::nullopt; // Clear error
                        retry_count++;

                        // Wait briefly before trying again
                        lock.unlock();
                        std::this_thread::sleep_for(std::chrono::milliseconds(500));
                        lock.lock();

                        // Try connecting again
                        if (pimpl->pipe_socket_unix_) {
                            lock.unlock();
                            pimpl->do_connect_pipe();
                            lock.lock();
                        }
                        continue;
                    }
                }
#endif

                // Throw exception for other errors
                throw rpc::system_error(*ec);
            }

            // Handle timeout
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
                        // Generate timeout error message and log it
                        if (pimpl->conn_type_ == client::impl::conn_type::TCP) {
                            std::string err_msg = RPCLIB_FMT::format(
                                    "Timeout of {}ms while connecting to {}:{} (after {} retries)",
                                    *timeout, pimpl->addr_, pimpl->port_, retry_count);
                            LOG_ERROR(err_msg);
                            std::cerr << err_msg << std::endl;
                            throw rpc::timeout(err_msg);
                        } else {
                            std::string err_msg = RPCLIB_FMT::format(
                                    "Timeout of {}ms while connecting to named pipe '{}' (after {} retries)",
                                    *timeout, pimpl->pipe_name_, retry_count);
                            LOG_ERROR(err_msg);
                            std::cerr << err_msg << std::endl;
                            throw rpc::timeout(err_msg);
                        }
                    }

                    LOG_WARN("Connection attempt {} timed out, retrying...", retry_count);
                    std::cout << "Connection attempt " << retry_count << " timed out, retrying..." << std::endl;

                    // Try connecting again
                    if (pimpl->conn_type_ == client::impl::conn_type::NAMED_PIPE) {
                        // Unlock before reconnecting
                        lock.unlock();
                        pimpl->do_connect_pipe();
                        lock.lock();
                    }
                    continue;
                }
            } else {
                // If no timeout set, use a reasonable default timeout
                auto now = std::chrono::steady_clock::now();
                if (now >= wait_end_time) {
                    std::string err_msg = "Connection timed out after maximum wait period";
                    LOG_ERROR(err_msg);
                    std::cerr << err_msg << std::endl;
                    throw rpc::timeout(err_msg);
                }

                // Wait at most 1 second at a time, then check status
                auto wait_status = pimpl->conn_finished_.wait_for(lock, std::chrono::seconds(1));
                if (wait_status == std::cv_status::timeout) {
                    LOG_WARN("Still waiting for connection... (retry {}/{})", retry_count + 1, max_retries);
                    std::cout << "Still waiting for connection... (retry " << (retry_count + 1)  
                              << "/" << max_retries << ")" << std::endl;
                    retry_count++;

                    // Try connecting again
                    if (retry_count < max_retries && pimpl->conn_type_ == client::impl::conn_type::NAMED_PIPE) {
                        // Unlock before reconnecting
                        lock.unlock();
                        pimpl->do_connect_pipe();
                        lock.lock();
                    }
                }
            }

            // Check connection status again
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
        pimpl->strand_.post([=]() {
            // Log the function call for better debugging
            if (enable_message_tracking || enable_pipe_debug) {
                LOG_INFO("Posting RPC call '{}' with ID {}", func_name, idx);
                std::cout << "Posting RPC call '" << func_name << "' with ID " << idx << std::endl;
            }
            
            pimpl->ongoing_calls_.insert(
                    std::make_pair(idx, std::make_pair(func_name, std::move(*p))));
            pimpl->write(std::move(*buffer));
        });
    }

    void client::post(RPCLIB_MSGPACK::sbuffer *buffer) {
        pimpl->strand_.post([=]() {
            pimpl->write(std::move(*buffer));
            delete buffer;
        });
    }

    client::connection_state client::get_connection_state() const {
        return pimpl->get_connection_state();
    }

    client::connection_type client::get_connection_type() const {
        return pimpl->get_connection_type();
    }

    nonstd::optional<int64_t> client::get_timeout() const {
        return pimpl->get_timeout();
    }

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
        pimpl->io_.stop();
        pimpl->io_thread_.join();
#ifdef _WIN32
        // Close Named Pipe handle on Windows platform
        if (pimpl->pipe_handle_ != INVALID_HANDLE_VALUE) {
            CloseHandle(pimpl->pipe_handle_);
            pimpl->pipe_handle_ = INVALID_HANDLE_VALUE;
        }
#endif
    }
} // namespace rpc
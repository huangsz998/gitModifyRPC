#include "rpc/client.h"
#include "rpc/config.h"
#include "rpc/rpc_error.h"

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

#ifndef _WIN32
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#endif

using namespace RPCLIB_ASIO;
using RPCLIB_ASIO::ip::tcp;
using namespace rpc::detail;

namespace rpc {

static constexpr uint32_t default_buffer_size =
    rpc::constants::DEFAULT_BUFFER_SIZE;

struct client::impl {
    // 连接类型枚举 - 内部使用
    enum class conn_type { TCP, NAMED_PIPE };
    
    // TCP构造函数
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
    
    // Named Pipe构造函数
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

#ifdef _WIN32
        // Windows平台 - 创建基于Windows命名管道的socket
        pipe_socket_win_ = detail::make_unique<local::windows::stream_protocol::socket>(io_);
#else
        // Unix/Linux平台 - 创建基于Unix Domain Socket的socket
        pipe_socket_unix_ = detail::make_unique<local::stream_protocol::socket>(io_);
#endif
    }

    // TCP连接函数
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
    
    // Named Pipe连接函数
    void do_connect_pipe() {
        LOG_INFO("Initiating Named Pipe connection to '{}'.", pipe_name_);
        connection_ec_ = nonstd::nullopt;
        
        try {
#ifdef _WIN32
            // 确保 pipe_socket_win_ 已被正确初始化
            if (!pipe_socket_win_) {
                pipe_socket_win_ = detail::make_unique<local::windows::stream_protocol::socket>(io_);
            }
            
            // Windows平台处理命名管道检查
            // Windows命名管道只在尝试连接时才能检查有效性
            
            // Windows平台 - 连接到Windows Named Pipe
            pipe_socket_win_->async_connect(
                local::windows::stream_protocol::endpoint("\\\\.\\pipe\\" + pipe_name_),
                strand_.wrap([this](std::error_code ec) {
                    if (!ec) {
                        std::unique_lock<std::mutex> lock(mut_connection_finished_);
                        LOG_INFO("Client connected to Named Pipe: {}", pipe_name_);
                        
                        try {
                            // 创建模板化的writer实例
                            writer_pipe_win_ = std::make_shared<detail::async_writer<local::windows::stream_protocol::socket>>(
                                &io_, std::move(*pipe_socket_win_));
                                
                            // 连接成功才设置状态标志
                            is_connected_ = true;
                            state_ = client::connection_state::connected;
                            conn_finished_.notify_all();
                            
                            do_read_pipe_win();
                        } catch (const std::exception &e) {
                            LOG_ERROR("Error during writer creation: {}", e.what());
                            connection_ec_ = std::make_error_code(std::errc::io_error);
                            state_ = client::connection_state::disconnected;
                            conn_finished_.notify_all();
                        }
                    } else {
                        std::unique_lock<std::mutex> lock(mut_connection_finished_);
                        LOG_ERROR("Error during Named Pipe connection: {}", ec);
                        state_ = client::connection_state::disconnected;
                        connection_ec_ = ec;
                        conn_finished_.notify_all();
                    }
                }));
#else
            // Unix/Linux平台 - 连接到Unix Domain Socket
            
            // 确保 pipe_socket_unix_ 已被正确初始化
            if (!pipe_socket_unix_) {
                pipe_socket_unix_ = detail::make_unique<local::stream_protocol::socket>(io_);
            }
            
            // 检查套接字文件是否存在并有效
            struct stat buffer;
            bool file_exists = (stat(pipe_name_.c_str(), &buffer) == 0);
            
            if (!file_exists) {
                // 文件不存在，抛出异常
                std::string err_msg = RPCLIB_FMT::format(
                    "Unix Domain Socket file does not exist: {}", pipe_name_);
                LOG_ERROR(err_msg);
                throw std::runtime_error(err_msg);
            } else if (!S_ISSOCK(buffer.st_mode)) {
                // 文件存在但不是套接字，抛出异常
                std::string err_msg = RPCLIB_FMT::format(
                    "Path exists but is not a valid socket: {}", pipe_name_);
                LOG_ERROR(err_msg);
                throw std::runtime_error(err_msg);
            } else {
                LOG_INFO("Found valid socket file: {}", pipe_name_);
                
                // 文件检查通过，尝试连接
                pipe_socket_unix_->async_connect(
                    local::stream_protocol::endpoint(pipe_name_),
                    strand_.wrap([this](std::error_code ec) {
                        if (!ec) {
                            std::unique_lock<std::mutex> lock(mut_connection_finished_);
                            LOG_INFO("Client connected to Unix Domain Socket: {}", pipe_name_);
                            
                            try {
                                // 创建模板化的writer实例
                                writer_pipe_unix_ = std::make_shared<detail::async_writer<local::stream_protocol::socket>>(
                                    &io_, std::move(*pipe_socket_unix_));
                                    
                                // 连接成功才设置状态标志
                                is_connected_ = true;
                                state_ = client::connection_state::connected;
                                conn_finished_.notify_all();
                                
                                do_read_pipe_unix();
                            } catch (const std::exception &e) {
                                LOG_ERROR("Error during writer creation: {}", e.what());
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
        } catch (const std::exception &e) {
            LOG_ERROR("Exception during connection setup: {}", e.what());
            std::unique_lock<std::mutex> lock(mut_connection_finished_);
            state_ = client::connection_state::disconnected;
            connection_ec_ = std::make_error_code(std::errc::io_error);
            conn_finished_.notify_all();
            // 重新抛出异常，确保它被传播到调用方
            throw;
        }
    }

    // TCP读取函数
    void do_read_tcp() {
        LOG_TRACE("do_read_tcp");
        constexpr std::size_t max_read_bytes = default_buffer_size;
        
        // 使用TCP专用writer
        writer_tcp_->socket().async_read_some(
            RPCLIB_ASIO::buffer(pac_.buffer(), max_read_bytes),
            [this, max_read_bytes](std::error_code ec, std::size_t length) {
                handle_read(ec, length, max_read_bytes, [this](){ do_read_tcp(); });
            });
    }
    
#ifdef _WIN32
    // Windows命名管道读取函数
    void do_read_pipe_win() {
        LOG_TRACE("do_read_pipe_win");
        constexpr std::size_t max_read_bytes = default_buffer_size;
        
        // 使用Windows命名管道专用writer
        writer_pipe_win_->socket().async_read_some(
            RPCLIB_ASIO::buffer(pac_.buffer(), max_read_bytes),
            [this, max_read_bytes](std::error_code ec, std::size_t length) {
                handle_read(ec, length, max_read_bytes, [this](){ do_read_pipe_win(); });
            });
    }
#else
    // Unix Domain Socket读取函数
    void do_read_pipe_unix() {
        LOG_TRACE("do_read_pipe_unix");
        constexpr std::size_t max_read_bytes = default_buffer_size;
        
        // 使用Unix Domain Socket专用writer
        writer_pipe_unix_->socket().async_read_some(
            RPCLIB_ASIO::buffer(pac_.buffer(), max_read_bytes),
            [this, max_read_bytes](std::error_code ec, std::size_t length) {
                handle_read(ec, length, max_read_bytes, [this](){ do_read_pipe_unix(); });
            });
    }
#endif

    // 通用读取处理函数
    template<typename ReadAgainFunc>
    void handle_read(std::error_code ec, std::size_t length, std::size_t max_read_bytes, ReadAgainFunc read_again) {
        if (!ec) {
            LOG_TRACE("Read chunk of size {}", length);
            pac_.buffer_consumed(length);

            RPCLIB_MSGPACK::unpacked result;
            while (pac_.next(result)) {
                auto r = response(std::move(result));
                auto id = r.get_id();
                auto &current_call = ongoing_calls_[id];
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
    
    // 获取连接类型
    client::connection_type get_connection_type() const {
        return conn_type_ == conn_type::TCP ? 
               client::connection_type::TCP : 
               client::connection_type::NAMED_PIPE;
    }

    // 写入函数，根据连接类型选择不同的writer
    void write(RPCLIB_MSGPACK::sbuffer item) {
        if (conn_type_ == conn_type::TCP) {
            writer_tcp_->write(std::move(item));
        } 
#ifdef _WIN32
        else if (writer_pipe_win_) {
            writer_pipe_win_->write(std::move(item));
        }
#else
        else if (writer_pipe_unix_) {
            writer_pipe_unix_->write(std::move(item));
        }
#endif
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
    
    // 连接信息
    std::string addr_;
    uint16_t port_;
    std::string pipe_name_;
    
    RPCLIB_MSGPACK::unpacker pac_;
    std::atomic_bool is_connected_;
    std::condition_variable conn_finished_;
    std::mutex mut_connection_finished_;
    std::thread io_thread_;
    std::atomic<client::connection_state> state_;
    
    // 连接类型
    conn_type conn_type_;
    
    // Named Pipe相关成员
#ifdef _WIN32
    std::unique_ptr<local::windows::stream_protocol::socket> pipe_socket_win_;
    std::shared_ptr<detail::async_writer<local::windows::stream_protocol::socket>> writer_pipe_win_;
#else
    std::unique_ptr<local::stream_protocol::socket> pipe_socket_unix_;
    std::shared_ptr<detail::async_writer<local::stream_protocol::socket>> writer_pipe_unix_;
#endif
    
    // TCP相关成员
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
        // 设置默认超时，避免永久等待
        set_timeout(5000); // 默认5秒超时
        
        // 在Linux上检查和处理Socket文件
#ifndef _WIN32
        // 在进行任何连接尝试前，直接检查文件是否存在且为有效套接字
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

        // 文件检查通过后开始连接
        pimpl->do_connect_pipe();
        std::thread io_thread([this]() {
            try {
                RPCLIB_CREATE_LOG_CHANNEL(client)
                name_thread("client_pipe");
                pimpl->io_.run();
            } catch (const std::exception &e) {
                LOG_ERROR("Exception in io_thread: {}", e.what());
            }
        });
        pimpl->io_thread_ = std::move(io_thread);
        
        try {
            LOG_INFO("Trying initial connection...");
            wait_conn();  // 尝试立即连接
        } catch (const rpc::timeout &) {
            LOG_WARN("Initial connection timed out, will retry later");
            // 超时可以忽略，后续调用会重试
        } catch (const std::runtime_error &e) {
            // 对于文件不存在或无效的错误，我们不希望重试
            LOG_ERROR("Critical connection error: {}", e.what());
            throw; // 重新抛出，确保客户端初始化失败
        } catch (const std::exception &e) {
            LOG_WARN("Initial connection failed: {}, will retry later", e.what());
            // 其他异常也可以忽略，后续调用会重试
        }
        
    } catch (const std::exception &e) {
        LOG_ERROR("Failed to initialize client: {}", e.what());
        throw;
    }
}


void client::wait_conn() {
    std::unique_lock<std::mutex> lock(pimpl->mut_connection_finished_);
    
    // 如果已连接，则直接返回
    if (pimpl->is_connected_) {
        return;
    }
    
    // 增加超时保护，避免永久阻塞
    const auto max_wait_time = std::chrono::seconds(10); // 最多等待10秒
    auto wait_end_time = std::chrono::steady_clock::now() + max_wait_time;
    
    int retry_count = 0;
    const int max_retries = 3;

    while (!pimpl->is_connected_ && retry_count < max_retries) {
        // 检查是否已经有错误
        if (auto ec = pimpl->connection_ec_) {
            LOG_ERROR("Connection error: {} ({})", ec->value(), ec->message());
            
#ifndef _WIN32
            // 对于Unix Domain Socket，尝试处理特定错误
            if (pimpl->conn_type_ == client::impl::conn_type::NAMED_PIPE) {
            
                if (ec->value() == static_cast<int>(std::errc::connection_refused) || 
                    ec->value() == static_cast<int>(std::errc::no_such_file_or_directory)) {
                    LOG_WARN("Socket connection refused or file not found, server may not be ready");
                    // 继续尝试，不抛出异常
                    pimpl->connection_ec_ = nonstd::nullopt; // 清除错误
                    retry_count++;
                    
                    // 短暂等待后重新尝试连接
                    lock.unlock();
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                    lock.lock();
                    
                    // 重新尝试连接
                    if (pimpl->pipe_socket_unix_) {
                        lock.unlock();
                        pimpl->do_connect_pipe();
                        lock.lock();
                    }
                    continue;
                }
            }
#endif
            
            // 其他错误则抛出异常
            throw rpc::system_error(*ec);
        }

        // 处理超时
        if (auto timeout = pimpl->timeout_) {
            LOG_DEBUG("Waiting for connection with timeout {}ms (retry {}/{})",
                     *timeout, retry_count + 1, max_retries);
            
            auto result = pimpl->conn_finished_.wait_for(
                lock, std::chrono::milliseconds(*timeout));
                
            if (result == std::cv_status::timeout) {
                retry_count++;
                
                if (retry_count >= max_retries) {
                    // 生成超时错误信息并记录日志
                    if (pimpl->conn_type_ == client::impl::conn_type::TCP) {
                        std::string err_msg = RPCLIB_FMT::format(
                            "Timeout of {}ms while connecting to {}:{} (after {} retries)",
                            *timeout, pimpl->addr_, pimpl->port_, retry_count);
                        LOG_ERROR(err_msg);
                        throw rpc::timeout(err_msg);
                    } else {
                        std::string err_msg = RPCLIB_FMT::format(
                            "Timeout of {}ms while connecting to named pipe '{}' (after {} retries)",
                            *timeout, pimpl->pipe_name_, retry_count);
                        LOG_ERROR(err_msg);
                        throw rpc::timeout(err_msg);
                    }
                }
                
                LOG_WARN("Connection attempt {} timed out, retrying...", retry_count);
                
                // 重新尝试连接
                if (pimpl->conn_type_ == client::impl::conn_type::NAMED_PIPE) {
#ifdef _WIN32
                    if (pimpl->pipe_socket_win_) {
                        lock.unlock();
                        pimpl->do_connect_pipe();
                        lock.lock();
                    }
#else
                    if (pimpl->pipe_socket_unix_) {
                        lock.unlock();
                        pimpl->do_connect_pipe();
                        lock.lock();
                    }
#endif
                }
                continue;
            }
        } else {
            // 如果没有设置超时，使用一个合理的默认超时
            auto now = std::chrono::steady_clock::now();
            if (now >= wait_end_time) {
                std::string err_msg = "Connection timed out after maximum wait period";
                LOG_ERROR(err_msg);
                throw rpc::timeout(err_msg);
            }
            
            // 每次最多等待1秒，然后检查状态
            auto wait_status = pimpl->conn_finished_.wait_for(lock, std::chrono::seconds(1));
            if (wait_status == std::cv_status::timeout) {
                LOG_WARN("Still waiting for connection... (retry {}/{})", retry_count + 1, max_retries);
                retry_count++;
                
                // 重新尝试连接
                if (retry_count < max_retries && pimpl->conn_type_ == client::impl::conn_type::NAMED_PIPE) {
#ifdef _WIN32
                    if (pimpl->pipe_socket_win_) {
                        lock.unlock();
                        pimpl->do_connect_pipe();
                        lock.lock();
                    }
#else
                    if (pimpl->pipe_socket_unix_) {
                        lock.unlock();
                        pimpl->do_connect_pipe();
                        lock.lock();
                    }
#endif
                }
            }
        }
        
        // 重新检查连接状态
        if (pimpl->is_connected_) {
            LOG_INFO("Connected successfully");
            return;
        }
    }
    
    if (!pimpl->is_connected_) {
        std::string err_msg = "Failed to connect after multiple attempts";
        LOG_ERROR(err_msg);
        throw rpc::timeout(err_msg);
    }
}

int client::get_next_call_idx() { return ++(pimpl->call_idx_); }

void client::post(std::shared_ptr<RPCLIB_MSGPACK::sbuffer> buffer, int idx,
                  std::string const &func_name,
                  std::shared_ptr<rsp_promise> p) {
    pimpl->strand_.post([=]() {
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
}

} // namespace rpc
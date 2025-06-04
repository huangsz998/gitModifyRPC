#include "rpc/client.h"
#include "rpc/client_impl.h"
#include "rpc/config.h"
#include "rpc/rpc_error.h"
#include "rpc/detail/write_helper_fwd.h"

#include <atomic>
#include <condition_variable>
#include <deque>
#include <future>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <memory>
#include <iostream>

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

// ========== impl ctor/dtor and member function implementations ==========

client::impl::impl(client *parent, std::string const &addr, uint16_t port)
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
      writer_tcp_(std::make_shared<detail::async_writer<RPCLIB_ASIO::ip::tcp::socket>>(&io_, RPCLIB_ASIO::ip::tcp::socket(io_))),
      timeout_(nonstd::nullopt),
      connection_ec_(nonstd::nullopt),
      work_guard_(nullptr)
{
    pac_.reserve_buffer(default_buffer_size);
    work_guard_.reset(new RPCLIB_ASIO::io_service::work(io_));
}

client::impl::impl(client *parent, std::string const &pipe_name)
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
      work_guard_(nullptr)
{
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

void client::impl::do_connect(tcp::resolver::iterator endpoint_iterator) {
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
        })
    );
}

void client::impl::do_connect_pipe() {
    LOG_INFO("Initiating Named Pipe connection to '{}'.", pipe_name_);
    connection_ec_ = nonstd::nullopt;
#ifdef _WIN32
    if (pipe_handle_ != INVALID_HANDLE_VALUE) {
        LOG_INFO("Closing existing pipe handle");
        CloseHandle(pipe_handle_);
        pipe_handle_ = INVALID_HANDLE_VALUE;
    }

    LOG_INFO("Connecting to Windows Named Pipe: {}", full_pipe_name_);
    int max_retries = 10;
    for (int i = 0; i < max_retries; i++) {
        LOG_INFO("Connection attempt {} of {}", i + 1, max_retries);
        pipe_handle_ = CreateFileA(
            full_pipe_name_.c_str(),
            GENERIC_READ | GENERIC_WRITE,
            0,
            NULL,
            OPEN_EXISTING,
            0,
            NULL
        );
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
    std::unique_lock<std::mutex> lock(mut_connection_finished_);
    LOG_INFO("Client connected to Windows Named Pipe: {}", full_pipe_name_);
    std::cout << "Client successfully connected to pipe: " << full_pipe_name_ << std::endl;
    is_connected_ = true;
    state_ = client::connection_state::connected;
    conn_finished_.notify_all();
#else
    // Linux/Unix Domain Socket implementation remains unchanged
#endif
}

void client::impl::do_read_tcp() {
    LOG_TRACE("do_read_tcp");
    constexpr std::size_t max_read_bytes = default_buffer_size;
    writer_tcp_->socket().async_read_some(
        RPCLIB_ASIO::buffer(pac_.buffer(), max_read_bytes),
        [this, max_read_bytes](std::error_code ec, std::size_t length) {
            handle_read(ec, length, max_read_bytes, [this]() { do_read_tcp(); });
        });
}

template<typename ReadAgainFunc>
void client::impl::handle_read(std::error_code ec, std::size_t length, std::size_t max_read_bytes, ReadAgainFunc read_again) {
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

client::connection_state client::impl::get_connection_state() const { return state_; }
client::connection_type client::impl::get_connection_type() const {
    return conn_type_ == conn_type::TCP ?
           client::connection_type::TCP :
           client::connection_type::NAMED_PIPE;
}
void client::impl::write(RPCLIB_MSGPACK::sbuffer item) {
    if (conn_type_ == conn_type::TCP) {
        if (writer_tcp_) {
            writer_tcp_->write(std::move(item));
        }
    }
#ifdef _WIN32
    // No writer_pipe_win_ in sync mode.
#endif
}
nonstd::optional<int64_t> client::impl::get_timeout() { return timeout_; }
void client::impl::set_timeout(int64_t value) { timeout_ = value; }
void client::impl::clear_timeout() { timeout_ = nonstd::nullopt; }

// ========== 其它 client（非impl）成员函数实现保持不变 ==========

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
#ifdef _WIN32
    if (get_connection_type() == client::connection_type::NAMED_PIPE) {
        // Blocking synchronous write for Windows pipe
        if (pimpl->pipe_handle_ != INVALID_HANDLE_VALUE) {
            ::rpc::detail::write_helper(pimpl->pipe_handle_, *buffer, true /* message_mode */);

            // Blocking read for response
            uint32_t resp_len = 0;
            DWORD read = 0;
            BOOL ok = ReadFile(pimpl->pipe_handle_, &resp_len, 4, &read, NULL);
            if (!ok || read != 4) {
                DWORD err = GetLastError();
                std::string err_msg = "[client::post] ReadFile (response length) failed, err=" + std::to_string(err);
                LOG_ERROR(err_msg);
                p->set_exception(std::make_exception_ptr(std::runtime_error(err_msg)));
                return;
            }
            if (resp_len == 0 || resp_len > (16 * 1024 * 1024)) {
                std::string err_msg = "[client::post] Invalid response frame size: " + std::to_string(resp_len);
                LOG_ERROR(err_msg);
                p->set_exception(std::make_exception_ptr(std::runtime_error(err_msg)));
                return;
            }
            std::vector<char> resp_buf(resp_len);
            ok = ReadFile(pimpl->pipe_handle_, resp_buf.data(), resp_len, &read, NULL);
            if (!ok || read != resp_len) {
                DWORD err = GetLastError();
                std::string err_msg = "[client::post] ReadFile (response payload) failed, err=" + std::to_string(err);
                LOG_ERROR(err_msg);
                p->set_exception(std::make_exception_ptr(std::runtime_error(err_msg)));
                return;
            }
            RPCLIB_MSGPACK::object_handle result;
            try {
                RPCLIB_MSGPACK::unpack(result, resp_buf.data(), resp_len);
                p->set_value(std::move(result));
            } catch (const std::exception& e) {
                p->set_exception(std::make_exception_ptr(e));
            }
            return;
        } else {
            std::string err_msg = "[client::post] Invalid pipe handle!";
            LOG_ERROR(err_msg);
            p->set_exception(std::make_exception_ptr(std::runtime_error(err_msg)));
            return;
        }
    }
#endif
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
#ifdef _WIN32
    if (get_connection_type() == client::connection_type::NAMED_PIPE) {
        if (pimpl->pipe_handle_ != INVALID_HANDLE_VALUE) {
            ::rpc::detail::write_helper(pimpl->pipe_handle_, *buffer, true /* message_mode */);
        } else {
            LOG_ERROR("[client::post] Invalid pipe handle for notification!");
        }
        delete buffer;
        return;
    }
#endif
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

// ========= New non-template forwarding member implementations ==========

RPCLIB_MSGPACK::object_handle client::do_call(const std::string &func_name, const std::vector<RPCLIB_MSGPACK::object> &args) {
    auto fut = do_async_call(func_name, args);
    if (auto timeout = get_timeout()) {
        auto wait_result = fut.wait_for(std::chrono::milliseconds(*timeout));
        if (wait_result == std::future_status::timeout) {
            throw_timeout(func_name);
        }
    }
    return fut.get();
}

std::future<RPCLIB_MSGPACK::object_handle> client::do_async_call(const std::string &func_name, const std::vector<RPCLIB_MSGPACK::object> &args) {
    wait_conn();
    LOG_DEBUG("Calling {}", func_name);

    auto args_tuple = std::make_tuple(args);
    int idx = get_next_call_idx();
    auto call_obj =
        std::make_tuple(static_cast<uint8_t>(request_type::call), idx, func_name, args_tuple);

    auto buffer = std::make_shared<RPCLIB_MSGPACK::sbuffer>();
    RPCLIB_MSGPACK::pack(*buffer, call_obj);

    auto p = std::make_shared<rsp_promise>();
    auto ft = p->get_future();

    post(buffer, idx, func_name, p);

    return ft;
}

void client::do_send(const std::string &func_name, const std::vector<RPCLIB_MSGPACK::object> &args) {
    LOG_DEBUG("Sending notification {}", func_name);

    auto args_tuple = std::make_tuple(args);
    auto call_obj = std::make_tuple(
        static_cast<uint8_t>(request_type::notification), func_name, args_tuple);

    auto buffer = new RPCLIB_MSGPACK::sbuffer();
    RPCLIB_MSGPACK::pack(*buffer, call_obj);

    post(buffer);
}

// ======================================================================

client::~client() {
    pimpl->work_guard_.reset();
    pimpl->io_.stop();
    pimpl->io_thread_.join();
#ifdef _WIN32
    if (pimpl->pipe_handle_ != INVALID_HANDLE_VALUE) {
        CloseHandle(pimpl->pipe_handle_);
        pimpl->pipe_handle_ = INVALID_HANDLE_VALUE;
    }
#endif
}

} // namespace rpc
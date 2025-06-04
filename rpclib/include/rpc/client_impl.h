#pragma once
#include <atomic>
#include <condition_variable>
#include <deque>
#include <future>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <memory>
#include <string>
#include <vector>
#include <system_error>
#include "nonstd/optional.hpp"
#include "rpc/msgpack.hpp"
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

namespace rpc {
class client;

struct client::impl {
    enum class conn_type { TCP, NAMED_PIPE };

    impl(client *parent, std::string const &addr, uint16_t port);
    impl(client *parent, std::string const &pipe_name);

    void do_connect(RPCLIB_ASIO::ip::tcp::resolver::iterator endpoint_iterator);
    void do_connect_pipe();
    void do_read_tcp();

    template<typename ReadAgainFunc>
    void handle_read(std::error_code ec, std::size_t length, std::size_t max_read_bytes, ReadAgainFunc read_again);

    client::connection_state get_connection_state() const;
    client::connection_type get_connection_type() const;
    void write(RPCLIB_MSGPACK::sbuffer item);
    nonstd::optional<int64_t> get_timeout();
    void set_timeout(int64_t value);
    void clear_timeout();

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

} // namespace rpc
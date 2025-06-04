#pragma once

#include <future>
#include <memory>
#include "nonstd/optional.hpp"
#include "rpc/config.h"
#include "rpc/detail/log.h"
#include "rpc/detail/pimpl.h"
#include "rpc/msgpack.hpp"

namespace rpc {

extern bool enable_pipe_debug;
extern bool enable_message_tracking;

void enable_pipe_debugging(bool enable = true);

namespace detail {
template<typename SocketType>
class async_writer;
}

class client {
public:
    enum class connection_type {
        TCP,
        NAMED_PIPE
    };

    client(std::string const &addr, uint16_t port);
    explicit client(std::string const &pipe_name);
    client(client const &) = delete;
    ~client();

    template <typename... Args>
    RPCLIB_MSGPACK::object_handle call(std::string const &func_name, Args... args);

    template <typename... Args>
    std::future<RPCLIB_MSGPACK::object_handle> async_call(std::string const &func_name, Args... args);

    template <typename... Args>
    void send(std::string const &func_name, Args... args);

    nonstd::optional<int64_t> get_timeout() const;
    void set_timeout(int64_t value);
    void clear_timeout();

    enum class connection_state { initial, connected, disconnected, reset };
    connection_state get_connection_state() const;
    connection_type get_connection_type() const;
    void wait_all_responses();
    void wait_conn();

private:
    using rsp_promise = std::promise<RPCLIB_MSGPACK::object_handle>;

    enum class request_type { call = 0, notification = 2 };

    // New non-template internal forwarding helpers
    RPCLIB_MSGPACK::object_handle do_call(const std::string &func_name, const std::vector<RPCLIB_MSGPACK::object> &args);
    std::future<RPCLIB_MSGPACK::object_handle> do_async_call(const std::string &func_name, const std::vector<RPCLIB_MSGPACK::object> &args);
    void do_send(const std::string &func_name, const std::vector<RPCLIB_MSGPACK::object> &args);

    void post(std::shared_ptr<RPCLIB_MSGPACK::sbuffer> buffer, int idx, std::string const& func_name, std::shared_ptr<rsp_promise> p);
    void post(RPCLIB_MSGPACK::sbuffer *buffer);
    int get_next_call_idx();
    RPCLIB_NORETURN void throw_timeout(std::string const& func_name);

private:
    static constexpr double buffer_grow_factor = 1.8;
    RPCLIB_DECLARE_PIMPL()
};

// Template implementation: only does packing and forwards to non-template helpers
template <typename... Args>
RPCLIB_MSGPACK::object_handle client::call(std::string const &func_name, Args... args) {
    // Pack all arguments into msgpack::object
    std::vector<RPCLIB_MSGPACK::object> packed_args;
    RPCLIB_MSGPACK::zone z;
    int dummy[] = {0, (packed_args.push_back(RPCLIB_MSGPACK::object(args, z)), 0)...};
    (void)dummy;
    return do_call(func_name, packed_args);
}

template <typename... Args>
std::future<RPCLIB_MSGPACK::object_handle>
client::async_call(std::string const &func_name, Args... args) {
    std::vector<RPCLIB_MSGPACK::object> packed_args;
    RPCLIB_MSGPACK::zone z;
    int dummy[] = {0, (packed_args.push_back(RPCLIB_MSGPACK::object(args, z)), 0)...};
    (void)dummy;
    return do_async_call(func_name, packed_args);
}

template <typename... Args>
void client::send(std::string const &func_name, Args... args) {
    std::vector<RPCLIB_MSGPACK::object> packed_args;
    RPCLIB_MSGPACK::zone z;
    int dummy[] = {0, (packed_args.push_back(RPCLIB_MSGPACK::object(args, z)), 0)...};
    (void)dummy;
    do_send(func_name, packed_args);
}

} // namespace rpc
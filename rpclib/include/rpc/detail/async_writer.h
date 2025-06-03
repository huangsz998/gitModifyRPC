#pragma once

#ifndef ASYNC_WRITER_H_HQIRH28I
#define ASYNC_WRITER_H_HQIRH28I

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>

#include "asio.hpp"

#include "rpc/config.h"
#include "rpc/msgpack.hpp"

namespace rpc {
namespace detail {

//! \brief 改造为模板类以支持不同类型的socket
template <typename SocketType>
class async_writer : public std::enable_shared_from_this<async_writer<SocketType>> {
public:
    async_writer(RPCLIB_ASIO::io_service *io, SocketType socket)
        : socket_(std::move(socket)),
          write_strand_(*io),
          is_writing_(false),
          exit_(false),
          is_closed_(false) {}

    void write(RPCLIB_MSGPACK::sbuffer &&data);
    
    SocketType &socket();
    
    RPCLIB_ASIO::strand &write_strand();
    
    void close();
    
    bool is_closed() const;

protected:
    template <typename T>
    std::shared_ptr<T> shared_from_base() {
        return std::static_pointer_cast<T>(this->shared_from_this());
    }

    void write_handler(std::error_code ec, std::size_t transferred);

private:
    SocketType socket_;
    RPCLIB_ASIO::strand write_strand_;
    std::atomic_bool is_writing_;
    std::deque<RPCLIB_MSGPACK::sbuffer> write_queue_;
    std::atomic_bool exit_;
    std::atomic_bool is_closed_;
};

// 特化为原始类型，用于向后兼容
using async_writer_tcp = async_writer<RPCLIB_ASIO::ip::tcp::socket>;

// 模板实现部分
template <typename SocketType>
void async_writer<SocketType>::write(RPCLIB_MSGPACK::sbuffer &&data) {
    write_queue_.push_back(std::move(data));
    if (!is_writing_ && !is_closed_) {
        is_writing_ = true;
        auto self(this->shared_from_this());
        write_strand_.post([this, self]() {
            if (exit_) {
                return;
            }
            auto &item = write_queue_.front();
            socket_.async_write_some(
                RPCLIB_ASIO::buffer(item.data(), item.size()),
                write_strand_.wrap(
                    [this, self](std::error_code ec, std::size_t transferred) {
                        write_handler(ec, transferred);
                    }));
        });
    }
}

template <typename SocketType>
void async_writer<SocketType>::write_handler(std::error_code ec, std::size_t transferred) {
    if (exit_) {
        return;
    }
    
    if (!ec) {
        if (transferred < write_queue_.front().size()) {
            // sbuffer没有consume方法，我们需要创建一个新的缓冲区
            RPCLIB_MSGPACK::sbuffer new_buf;
            const size_t remaining = write_queue_.front().size() - transferred;
            new_buf.write(write_queue_.front().data() + transferred, remaining);
            write_queue_.front() = std::move(new_buf);
            
            if (exit_) {
                return;
            }
            auto &item = write_queue_.front();
            auto self(this->shared_from_this());
            socket_.async_write_some(
                RPCLIB_ASIO::buffer(item.data(), item.size()),
                write_strand_.wrap([this, self](
                    std::error_code ec, std::size_t transferred) {
                    write_handler(ec, transferred);
                }));
        } else {
            write_queue_.pop_front();
            if (write_queue_.empty()) {
                is_writing_ = false;
            } else {
                if (exit_) {
                    return;
                }
                auto &item = write_queue_.front();
                auto self(this->shared_from_this());
                socket_.async_write_some(
                    RPCLIB_ASIO::buffer(item.data(), item.size()),
                    write_strand_.wrap(
                        [this, self](std::error_code ec,
                                  std::size_t transferred) {
                            write_handler(ec, transferred);
                        }));
            }
        }
    } else {
        is_writing_ = false;
        if (ec != RPCLIB_ASIO::error::operation_aborted) {
            // Client connection is broken. Remove this client from the server
            close();
        }
    }
}

template <typename SocketType>
SocketType &async_writer<SocketType>::socket() {
    return socket_;
}

template <typename SocketType>
RPCLIB_ASIO::strand &async_writer<SocketType>::write_strand() {
    return write_strand_;
}

template <typename SocketType>
void async_writer<SocketType>::close() {
    exit_ = true;
    is_closed_ = true;
    std::error_code ec;
    socket_.shutdown(RPCLIB_ASIO::socket_base::shutdown_both, ec);
    socket_.close(ec);
}

template <typename SocketType>
bool async_writer<SocketType>::is_closed() const {
    return is_closed_;
}

} /* detail */
} /* rpc */

#endif /* end of include guard: ASYNC_WRITER_H_HQIRH28I */
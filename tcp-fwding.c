/*
 简短转发流程：

 1. 初始化（`pf_tcp_create`）
    - 初始化 Winsock、解析 IP、创建 IOCP、定时器队列和会话表。

 2. 启动（`pf_tcp_start`）
    - 创建监听 socket，获取 `AcceptEx`/`ConnectEx` 等扩展函数，绑定并关联到 IOCP，
      创建工作线程并通过 `post_accept` 向 IOCP 投递初始事件。

 3. 接受连接
    - 调用 `post_accept`，为每个新会话创建 `pf_session_t` 并调用 `AcceptEx`。

 4. Accept 完成（`handle_accept`）
    - 调用 `SO_UPDATE_ACCEPT_CONTEXT` 完成 accept，上报客户端地址（`get_accept_sockaddrs`）；
      如配置超时则 `post_timeout`，然后调用 `post_connect` 对目标发起 `ConnectEx`。

 5. Connect 完成（`handle_connect`）
    - 取消超时（`session_cancel_timeout`），调用 `SO_UPDATE_CONNECT_CONTEXT`，获取本地/远端地址；
      启动双方异步接收（`post_recv`）进入数据转发阶段。

 6. 数据转发（读/写循环）
    - 链式数据流：
      - 源读 -> 目标写 -> 源读
      - 目标读 -> 源写 -> 目标读
    - 读完成（`handle_*_read`）：若 bytes==0 标记半关闭，否则把数据 `post_send` 到对端。
    - 写完成（`handle_*_write`）：处理部分发送（调整 `WSABUF`）或发送完毕后重新 `post_recv`。
    - 使用引用计数和 `session_io_release` 安全地做半关闭或强制关闭。

 7. 超时与关闭
    - 定时器回调 `handle_timer_callback` 通过 IOCP 通知（区分 bytes=1 到期和 bytes=0 取消）；
      超时或错误时调用 `session_request_close`，最终由 `session_io_release`/`session_release` 回收资源。

 8. 停止与清理（`pf_tcp_stop` / 线程退出）
    - 停止时向 IOCP 投递 `completion_key == 0` 让工作线程退出；最后一个线程遍历 `session_table` 强制释放残留会话。

 额外说明：
 - IOCP `completion_key` 约定：0 = 退出，1 = 初始化/要求投递 Accept，其他 = 指向 `pf_tcp_t`（实际事件由 `OVERLAPPED` 的 `operation_type` 区分）。
 - 每个处理函数在开始时调用 `session_io_addref`，结束时调用 `session_io_release`，保证在最后一个 IO 完成时执行真正的关闭/半关闭逻辑。
 - 关键异步接口：`AcceptEx`、`ConnectEx`、`WSARecv`、`WSASend`、IOCP、TimerQueue。
*/

#define _CRT_SECURE_NO_WARNINGS

#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include <winsock2.h>
#include <ws2tcpip.h>
#include <mswsock.h>
#include <windows.h>

#include "table.h"
#include "fwding.h"

#pragma comment(lib, "ws2_32.lib")

// IO操作类型
typedef enum {
    PF_IO_NONE = 0,
    PF_IO_ACCEPT = 1,
    PF_IO_CONNECT = 2,
    PF_IO_SRC_READ = 3,
    PF_IO_SRC_WRITE = 4,
    PF_IO_DST_READ = 5,
    PF_IO_DST_WRITE = 6,
    PF_IO_TIMEOUT = 7,
} pf_io_type_t;

// 会话关闭类型
typedef enum {
    PF_SESSION_NONE = 0,
    PF_SESSION_CLOSE = 1,
    PF_SESSION_SRC_CLOSE = 2,
    PF_SESSION_DST_CLOSE = 4,
} pf_session_close_flag_t;

// 会话超时类型
typedef enum {
    PF_SESSION_TIMEOUT_NONE = 0,
    PF_SESSION_TIMEOUT_START = 1,
    PF_SESSION_TIMEOUT_COMPLETE = 2,
    PF_SESSION_TIMEOUT_END = 3,
} pf_session_timeout_type_t;

// 联合地址结构
typedef union pf_sockaddr_t {
    struct sockaddr addr;
    struct sockaddr_in sin4;
    struct sockaddr_in6 sin6;
} pf_sockaddr_t;

// 转发数据结构
typedef struct pf_tcp_t {
    // 运行状态
    volatile LONG running;        // 是否运行中
    volatile LONG active_threads; // 运行的线程数
    volatile LONG curr_accepts;   // 当前接受数量
    volatile LONG post_count;     // 投递数量

    // 工作线程
    HANDLE stop_event;            // 停止事件
    HANDLE thread_daemon;         // 守护线程
    HANDLE* thread_workers;       // 工作线程
    int total_accepts;            // 总接受数量
    int thread_count;             // 工作线程数量

    // 配置信息
    pf_config_t config;           // 转发信息
    pf_sockaddr_t src_addr;       // 地址数据
    pf_sockaddr_t out_addr;       // 地址数据
    pf_sockaddr_t dst_addr;       // 地址数据

    // 套接字相关
    HANDLE iocp;                    // IOCP句柄
    HANDLE timer_queue;             // 定时器队列
    table_t* session_table;         // 会话表
    SOCKET listen_sock;             // 监听套接字
    LPFN_ACCEPTEX acceptex;         // 扩展接口
    LPFN_CONNECTEX connectex;       // 扩展接口
    LPFN_DISCONNECTEX disconnectex; // 扩展接口
    LPFN_GETACCEPTEXSOCKADDRS get_accept_sockaddrs;
} pf_tcp_t;

// 重叠IO上下文
typedef struct pf_session_t pf_session_t;
typedef struct pf_io_context_t {
    OVERLAPPED overlapped;        // 重叠结构
    pf_io_type_t operation_type;  // 操作类型
    pf_session_t* session;        // 会话指针
} pf_io_context_t;

// 会话上下文
typedef struct pf_session_t {
    volatile LONG ref;            // 引用计数
    volatile LONG io_ref;         // IO引用计数
    volatile LONG close_flags;    // 关闭标志
    volatile LONG timeout_status; // 超时状态

    ssize_t session_id;           // 唯一ID
    HANDLE timeout_timer;         // 定时器句柄
    pf_tcp_t* pf;                 // 父指针

    SOCKET src_sock;              // 源套接字
    SOCKADDR* src_local_addr;     // 源本地地址
    SOCKADDR* src_remote_addr;    // 源远程地址
    WSABUF src_wsa_buffer;        // 源缓冲区

    SOCKET dst_sock;              // 目标套接字
    SOCKADDR* dst_local_addr;     // 目标本地地址
    SOCKADDR* dst_remote_addr;    // 目标远程地址
    WSABUF dst_wsa_buffer;        // 目标缓冲区

    // 重叠IO上下文
    pf_io_context_t src_io;       // 源IO上下文
    pf_io_context_t dst_io;       // 目标IO上下文
    pf_io_context_t tmo_io;       // 超时IO上下文

    // 缓冲区
    char src_addr_buffer[(sizeof(pf_sockaddr_t) + 16) * 2];
    char dst_addr_buffer[(sizeof(pf_sockaddr_t) + 16) * 2];
    char src_data_buffer[8192];
    char dst_data_buffer[8192];
} pf_session_t;

// 内部函数声明
static int post_accept(pf_tcp_t* pf);
static int post_connect(pf_tcp_t* pf, pf_session_t* session);
static int post_timeout(pf_tcp_t* pf, pf_session_t* session);
static int post_recv(pf_tcp_t* pf, pf_session_t* session, pf_io_type_t type);
static int post_send(pf_tcp_t* pf, pf_session_t* session, pf_io_type_t type, int bytes_transferred, int offset);
static int parse_ip_addr(const char* ip, unsigned short port, pf_sockaddr_t* addr);
static pf_session_t* session_create(pf_tcp_t* pf);
static void session_addref(pf_session_t* session);
static void session_io_addref(pf_session_t* session);
static void session_io_release(pf_session_t* session);
static void session_request_close(pf_session_t* session, pf_session_close_flag_t flag);
static void session_cancel_timeout(pf_session_t* session);
static void session_release(pf_session_t* session);
static void session_free(pf_session_t* session);
static DWORD WINAPI thread_daemon_callback(LPVOID param);
static DWORD WINAPI thread_worker_callback(LPVOID param);

// TCP转发创建
pf_tcp_t* pf_tcp_create(pf_config_t* config)
{
    pf_tcp_t* pf;
    WSADATA wsa_data;
    if (0 != WSAStartup(MAKEWORD(2, 2), &wsa_data))
    {
        goto ERROR_1;
    }

    pf = (pf_tcp_t*)malloc(sizeof(pf_tcp_t));
    if (NULL == pf)
    {
        goto ERROR_2;
    }

    memset(pf, 0, sizeof(pf_tcp_t));
    memcpy(&pf->config, config, sizeof(pf_config_t));

    SYSTEM_INFO sys_info;
    GetSystemInfo(&sys_info);
    pf->thread_count = sys_info.dwNumberOfProcessors * 2;
    pf->thread_workers = (HANDLE*)malloc(sizeof(HANDLE) * pf->thread_count);
    if (NULL == pf->thread_workers)
    {
        goto ERROR_3;
    }

    if (-1 == parse_ip_addr(pf->config.src_ip, pf->config.src_port, &pf->src_addr))
    {
        goto ERROR_4;
    }

    if (-1 == parse_ip_addr(pf->config.dst_ip, pf->config.dst_port, &pf->dst_addr))
    {
        goto ERROR_4;
    }

    if (0 == pf->config.out_ip[0])
    {
        if (AF_INET == pf->dst_addr.addr.sa_family)
        {
            strcpy(pf->config.out_ip, "0.0.0.0");
        }
        else if (AF_INET6 == pf->dst_addr.addr.sa_family)
        {
            strcpy(pf->config.out_ip, "::");
        }
    }

    if (-1 == parse_ip_addr(pf->config.out_ip, 0, &pf->out_addr))
    {
        goto ERROR_4;
    }

    pf->timer_queue = CreateTimerQueue();
    if (NULL == pf->timer_queue)
    {
        goto ERROR_4;
    }

    pf->session_table = table_create(16, 8192);
    if (NULL == pf->session_table)
    {
        goto ERROR_5;
    }

    pf->iocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
    if (NULL == pf->iocp)
    {
        goto ERROR_6;
    }

    pf->running = 0;
    pf->listen_sock = INVALID_SOCKET;
    pf->total_accepts = pf->thread_count * 2;
    if (pf->total_accepts < 32)
    {
        pf->total_accepts = 32;
    }

    return pf;

ERROR_6:
    table_close(pf->session_table);
ERROR_5:
    (void)DeleteTimerQueueEx(pf->timer_queue, INVALID_HANDLE_VALUE);
ERROR_4:
    free(pf->thread_workers);
ERROR_3:
    free(pf);
ERROR_2:
    WSACleanup();
ERROR_1:
    return NULL;
}

// TCP转发销毁
void pf_tcp_destroy(pf_tcp_t* pf)
{
    if (NULL == pf)
    {
        return;
    }

    if (0 != pf->running)
    {
        pf_tcp_stop(pf);
    }

    CloseHandle(pf->iocp);
    table_close(pf->session_table);
    (void)DeleteTimerQueueEx(pf->timer_queue, INVALID_HANDLE_VALUE);
    free(pf->thread_workers);
    free(pf);

    WSACleanup();
}

// TCP转发启动
int pf_tcp_start(pf_tcp_t* pf)
{
    DWORD bytes = 0;
    GUID guid_acceptex = WSAID_ACCEPTEX;
    GUID guid_connectex = WSAID_CONNECTEX;
    GUID guid_disconnectex = WSAID_DISCONNECTEX;
    GUID guid_get_acceptex_sockaddrs = WSAID_GETACCEPTEXSOCKADDRS;

    if (NULL == pf || 0 != pf->running)
    {
        goto ERROR_1;
    }

    // 创建停止事件
    pf->stop_event = CreateEventW(NULL, TRUE, FALSE, NULL);
    if (NULL == pf->stop_event)
    {
        goto ERROR_1;
    }

    // 创建TCP监听套接字
    pf->listen_sock = WSASocketW(pf->src_addr.addr.sa_family,
        SOCK_STREAM, IPPROTO_TCP, NULL, 0, WSA_FLAG_OVERLAPPED);
    if (INVALID_SOCKET == pf->listen_sock)
    {
        goto ERROR_2;
    }

    // 获取扩展函数
    if (SOCKET_ERROR == WSAIoctl(pf->listen_sock, SIO_GET_EXTENSION_FUNCTION_POINTER,
        &guid_acceptex, sizeof(GUID), &pf->acceptex, sizeof(FARPROC), &bytes, NULL, NULL))
    {
        goto ERROR_3;
    }

    // 获取扩展函数
    if (SOCKET_ERROR == WSAIoctl(pf->listen_sock, SIO_GET_EXTENSION_FUNCTION_POINTER,
        &guid_connectex, sizeof(GUID), &pf->connectex, sizeof(FARPROC), &bytes, NULL, NULL))
    {
        goto ERROR_3;
    }

    // 获取扩展函数
    if (SOCKET_ERROR == WSAIoctl(pf->listen_sock, SIO_GET_EXTENSION_FUNCTION_POINTER,
        &guid_disconnectex, sizeof(GUID), &pf->disconnectex, sizeof(FARPROC), &bytes, NULL, NULL))
    {
        goto ERROR_3;
    }

    // 获取扩展函数
    if (SOCKET_ERROR == WSAIoctl(pf->listen_sock, SIO_GET_EXTENSION_FUNCTION_POINTER,
        &guid_get_acceptex_sockaddrs, sizeof(GUID), &pf->get_accept_sockaddrs, sizeof(FARPROC), &bytes, NULL, NULL))
    {
        goto ERROR_3;
    }

    // 绑定监听套接字
    if (AF_INET == pf->src_addr.addr.sa_family)
    {
        // IPV4
        if (SOCKET_ERROR == bind(pf->listen_sock,
            &pf->src_addr.addr, sizeof(pf->src_addr.sin4)))
        {
            goto ERROR_3;
        }
    }
    else if (AF_INET6 == pf->src_addr.addr.sa_family)
    {
        // IPV6
        if (SOCKET_ERROR == bind(pf->listen_sock,
            &pf->src_addr.addr, sizeof(pf->src_addr.sin6)))
        {
            goto ERROR_3;
        }
    }
    else
    {
        goto ERROR_3;
    }

    // 将监听套接字与IOCP关联
    if (NULL == CreateIoCompletionPort((HANDLE)pf->listen_sock, pf->iocp, (ULONG_PTR)pf, 0))
    {
        goto ERROR_3;
    }

    // 监听开始
    if (SOCKET_ERROR == listen(pf->listen_sock, SOMAXCONN))
    {
        goto ERROR_3;
    }

    // 准备启动
    pf->running = 1;
    pf->post_count = 0;
    pf->curr_accepts = 0;
    pf->active_threads = 0;

    // 创建线程
    for (int i = 0; i < pf->thread_count; i++)
    {
        // 创建工作线程
        InterlockedIncrement(&pf->active_threads);
        pf->thread_workers[i] = CreateThread(NULL, 0, thread_worker_callback, pf, 0, NULL);
        if (NULL == pf->thread_workers[i])
        {
            // 错误处理
            InterlockedExchange(&pf->running, 0);
            InterlockedDecrement(&pf->active_threads);

            closesocket(pf->listen_sock);
            pf->listen_sock = INVALID_SOCKET;

            for (int j = 0; j < i; j++)
            {
                PostQueuedCompletionStatus(pf->iocp, 0, 0, NULL);
            }

            for (int j = 0; j < i; j++)
            {
                WaitForSingleObject(pf->thread_workers[j], INFINITE);
                CloseHandle(pf->thread_workers[j]);
                pf->thread_workers[j] = NULL;
            }

            CloseHandle(pf->stop_event);
            pf->stop_event = NULL;

            pf->curr_accepts = 0;
            goto ERROR_1;
        }
    }

    // 守护线程
    pf->thread_daemon = CreateThread(NULL, 0, thread_daemon_callback, pf, 0, NULL);
    if (NULL == pf->thread_daemon)
    {
        // 错误处理
        pf_tcp_stop(pf);
        goto ERROR_1;
    }

    return 0;

ERROR_3:
    closesocket(pf->listen_sock);
    pf->listen_sock = INVALID_SOCKET;
ERROR_2:
    CloseHandle(pf->stop_event);
    pf->stop_event = NULL;
ERROR_1:
    return -1;
}

// TCP转发停止
int pf_tcp_stop(pf_tcp_t* pf)
{
    if (NULL == pf || 0 == pf->running)
    {
        return -1;
    }

    InterlockedExchange(&pf->running, 0);

    if (NULL != pf->thread_daemon)
    {
        SetEvent(pf->stop_event);
        WaitForSingleObject(pf->thread_daemon, INFINITE);
        CloseHandle(pf->thread_daemon);
    }

    if (INVALID_SOCKET != pf->listen_sock)
    {
        closesocket(pf->listen_sock);
        pf->listen_sock = INVALID_SOCKET;
    }

    for (int i = 0; i < pf->thread_count; i++)
    {
        PostQueuedCompletionStatus(pf->iocp, 0, 0, NULL);
    }

    for (int i = 0; i < pf->thread_count; i++)
    {
        if (NULL != pf->thread_workers[i])
        {
            WaitForSingleObject(pf->thread_workers[i], INFINITE);
            CloseHandle(pf->thread_workers[i]);
            pf->thread_workers[i] = NULL;
        }
    }

    CloseHandle(pf->stop_event);
    pf->stop_event = NULL;
    pf->curr_accepts = 0;
    return 0;
}

// 接受连接
static void handle_accept(pf_tcp_t* pf, pf_io_context_t* context, int is_success)
{
    // 继续投递
    if (0 != InterlockedCompareExchange(&pf->running, 0, 0))
    {
        if (-1 == post_accept(pf))
        {
            InterlockedDecrement(&pf->curr_accepts);
        }
    }

    // 处理事件
    int local_addr_len, remote_addr_len;
    pf_session_t* session = context->session;
    session_io_addref(session);

    // 出错
    if (0 == is_success)
    {
        goto ERROR_1;
    }

    // 更新信息
    if (SOCKET_ERROR == setsockopt(session->src_sock, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT,
        (char*)&pf->listen_sock, sizeof(SOCKET)))
    {
        goto ERROR_1;
    }

    // 获取地址
    pf->get_accept_sockaddrs(session->src_addr_buffer, 0, sizeof(pf_sockaddr_t) + 16, sizeof(pf_sockaddr_t) + 16,
        &session->src_local_addr, &local_addr_len, &session->src_remote_addr, &remote_addr_len);

    // 连接超时
    if (pf->config.timeout_ms > 0)
    {
        if (-1 == post_timeout(pf, session))
        {
            goto ERROR_1;
        }
    }

    // 连接目标
    if (-1 == post_connect(pf, session))
    {
        goto ERROR_2;
    }

    goto RESULT;

ERROR_2:
    if (pf->config.timeout_ms > 0)
    {
        session_cancel_timeout(session);
    }
ERROR_1:
    session_request_close(session, PF_SESSION_CLOSE);

RESULT:
    session_io_release(session);
    session_release(session);
}

// 连接处理
static void handle_connect(pf_tcp_t* pf, pf_io_context_t* context, int is_success)
{
    int addr_buffer_len;
    pf_session_t* session = context->session;
    session_io_addref(session);

    // 取消连接超时计时器
    if (pf->config.timeout_ms > 0)
    {
        session_cancel_timeout(session);
    }

    // 出错
    if (0 == is_success)
    {
        goto ERROR_1;
    }

    // 更新信息
    if (SOCKET_ERROR == setsockopt(session->dst_sock, SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT, NULL, 0))
    {
        goto ERROR_1;
    }

    // 获取地址
    addr_buffer_len = sizeof(pf_sockaddr_t) + 16;
    if (SOCKET_ERROR == getsockname(session->dst_sock, (struct sockaddr*)session->dst_addr_buffer, &addr_buffer_len))
    {
        goto ERROR_1;
    }

    addr_buffer_len = sizeof(pf_sockaddr_t) + 16;
    if (SOCKET_ERROR == getpeername(session->dst_sock, (struct sockaddr*)(session->dst_addr_buffer + sizeof(pf_sockaddr_t) + 16), &addr_buffer_len))
    {
        goto ERROR_1;
    }

    // 设置地址指针
    session->dst_local_addr = (SOCKADDR*)(session->dst_addr_buffer);
    session->dst_remote_addr = (SOCKADDR*)(session->dst_addr_buffer + sizeof(pf_sockaddr_t) + 16);

    // 投递源数据接收
    if (-1 == post_recv(pf, session, PF_IO_SRC_READ))
    {
        goto ERROR_1;
    }

    // 投递目标数据接收
    if (-1 == post_recv(pf, session, PF_IO_DST_READ))
    {
        goto ERROR_1;
    }

    goto RESULT;

ERROR_1:
    session_request_close(session, PF_SESSION_CLOSE);

RESULT:
    session_io_release(session);
    session_release(session);
}

// 源接收处理
static void handle_src_read(pf_tcp_t* pf, pf_io_context_t* context, int is_success, int bytes_transferred)
{
    // 会话指针
    pf_session_t* session = context->session;
    session_io_addref(session);

    // 接收出错
    if (0 == is_success)
    {
        goto ERROR_1;
    }

    // 对方关闭连接
    if (0 == bytes_transferred)
    {
        session_request_close(session, PF_SESSION_SRC_CLOSE);
        goto RESULT;
    }

    // 投递目标发送
    if (-1 == post_send(pf, session, PF_IO_DST_WRITE, bytes_transferred, 0))
    {
        goto ERROR_1;
    }

    goto RESULT;

ERROR_1:
    session_request_close(session, PF_SESSION_CLOSE);

RESULT:
    session_io_release(session);
    session_release(session);
}

// 目标接收处理
static void handle_dst_read(pf_tcp_t* pf, pf_io_context_t* context, int is_success, int bytes_transferred)
{
    // 会话指针
    pf_session_t* session = context->session;
    session_io_addref(session);

    // 接收出错
    if (0 == is_success)
    {
        goto ERROR_1;
    }

    // 对方关闭连接
    if (0 == bytes_transferred)
    {
        session_request_close(session, PF_SESSION_DST_CLOSE);
        goto RESULT;
    }

    // 投递目标发送
    if (-1 == post_send(pf, session, PF_IO_SRC_WRITE, bytes_transferred, 0))
    {
        goto ERROR_1;
    }

    goto RESULT;

ERROR_1:
    session_request_close(session, PF_SESSION_CLOSE);

RESULT:
    session_io_release(session);
    session_release(session);
}

// 源发送处理
static void handle_src_write(pf_tcp_t* pf, pf_io_context_t* context, int is_success, int bytes_transferred)
{
    pf_session_t* session = context->session;
    session_io_addref(session);

    if (0 == is_success)
    {
        goto ERROR_1;
    }

    if (bytes_transferred < (int)session->dst_wsa_buffer.len)
    {
        // 发生部分发送，调整缓冲区并重新发送
        if (-1 == post_send(pf, session, PF_IO_SRC_WRITE, bytes_transferred, bytes_transferred))
        {
            goto ERROR_1;
        }
    }
    else
    {
        // 发送完成，可以接收来自目标的新数据了
        if (-1 == post_recv(pf, session, PF_IO_DST_READ))
        {
            goto ERROR_1;
        }
    }

    goto RESULT;

ERROR_1:
    session_request_close(session, PF_SESSION_CLOSE);

RESULT:
    session_io_release(session);
    session_release(session);
}

// 目标发送处理
static void handle_dst_write(pf_tcp_t* pf, pf_io_context_t* context, int is_success, int bytes_transferred)
{
    pf_session_t* session = context->session;
    session_io_addref(session);

    if (0 == is_success)
    {
        goto ERROR_1;
    }

    if (bytes_transferred < (int)session->src_wsa_buffer.len)
    {
        // 发生部分发送，调整缓冲区并重新发送
        if (-1 == post_send(pf, session, PF_IO_DST_WRITE, bytes_transferred, bytes_transferred))
        {
            goto ERROR_1;
        }
    }
    else
    {
        // 发送完成，可以接收来自源的新数据了
        if (-1 == post_recv(pf, session, PF_IO_SRC_READ))
        {
            goto ERROR_1;
        }
    }

    goto RESULT;

ERROR_1:
    session_request_close(session, PF_SESSION_CLOSE);

RESULT:
    session_io_release(session);
    session_release(session);
}

// 连接超时处理
static void handle_timeout(pf_tcp_t* pf, pf_io_context_t* context, int is_success, int bytes_transferred)
{
    pf_session_t* session = context->session;
    session_io_addref(session);

    if (0 != is_success && 0 != bytes_transferred)
    {
        session_request_close(session, PF_SESSION_CLOSE);
    }

    session_io_release(session);
    session_release(session);
}

// 定时器回调
static VOID CALLBACK handle_timer_callback(PVOID parameter, BOOLEAN timer_or_waitfired)
{
    pf_session_t* session = (pf_session_t*)parameter;
    if (PF_SESSION_TIMEOUT_START == InterlockedCompareExchange(&session->timeout_status, PF_SESSION_TIMEOUT_COMPLETE, PF_SESSION_TIMEOUT_START))
    {
        PostQueuedCompletionStatus(session->pf->iocp, 1, (ULONG_PTR)session->pf, &session->tmo_io.overlapped);
        InterlockedExchange(&session->timeout_status, PF_SESSION_TIMEOUT_NONE);
    }
}

// 守护线程
static DWORD WINAPI thread_daemon_callback(LPVOID param)
{
    pf_tcp_t* pf = (pf_tcp_t*)param;
    for (;;)
    {
        // 退出信号
        if (0 == InterlockedCompareExchange(&pf->running, 0, 0))
        {
            break;
        }

        for (;;)
        {
            // 投递数是否相等
            if (pf->total_accepts == InterlockedCompareExchange(
                &pf->curr_accepts, pf->total_accepts, pf->total_accepts))
            {
                break;
            }

            // 补充投递
            if (-1 != post_accept(pf))
            {
                // 投递成功
                InterlockedIncrement(&pf->curr_accepts);
            }
            else
            {
                break;
            }
        }

        // 让出CPU
        WaitForSingleObject(pf->stop_event, 1000);
    }

    return 0;
}

// 工作线程
static DWORD WINAPI thread_worker_callback(LPVOID param)
{
    pf_tcp_t* pf = (pf_tcp_t*)param;
    for (;;)
    {
        // 获取完成状态
        DWORD bytes_transferred;
        ULONG_PTR completion_key;
        LPOVERLAPPED overlapped;
        BOOL is_success = GetQueuedCompletionStatus(
            pf->iocp,
            &bytes_transferred,
            &completion_key,
            &overlapped,
            INFINITE
        );

        // 流程控制
        if (0 == completion_key)
        {
            // 退出信号
            break;
        }
        else if (NULL != overlapped)
        {
            // 基础上下文
            InterlockedDecrement(&pf->post_count);
            pf_io_context_t* context = (pf_io_context_t*)overlapped;
            if (PF_IO_ACCEPT == context->operation_type)
            {
                // 接受连接
                handle_accept(pf, context, is_success);
            }
            else if (PF_IO_CONNECT == context->operation_type)
            {
                // 客户连接
                handle_connect(pf, context, is_success);
            }
            else if (PF_IO_SRC_READ == context->operation_type)
            {
                // 接收数据
                handle_src_read(pf, context, is_success, (int)bytes_transferred);
            }
            else if (PF_IO_SRC_WRITE == context->operation_type)
            {
                // 发送数据
                handle_src_write(pf, context, is_success, (int)bytes_transferred);
            }
            else if (PF_IO_DST_READ == context->operation_type)
            {
                // 接收数据
                handle_dst_read(pf, context, is_success, (int)bytes_transferred);
            }
            else if (PF_IO_DST_WRITE == context->operation_type)
            {
                // 发送数据
                handle_dst_write(pf, context, is_success, (int)bytes_transferred);
            }
            else if (PF_IO_TIMEOUT == context->operation_type)
            {
                // 超时处理
                handle_timeout(pf, context, is_success, (int)bytes_transferred);
            }
        }
    }

    // 最后一个线程负责清理工作
    if (0 == InterlockedDecrement(&pf->active_threads))
    {
        // 关闭所有会话
        pf_session_t* curr_session = NULL;
        ssize_t count = table_max_id(pf->session_table) + 1;
        for (ssize_t i = 0; i < count; i++)
        {
            if (-1 != table_get(pf->session_table, i, (uintptr_t*)&curr_session))
            {
                session_io_addref(curr_session);
                session_request_close(curr_session, PF_SESSION_CLOSE);
                session_io_release(curr_session);
            }
        }

        // 释放内存
        while (0 != InterlockedCompareExchange(&pf->post_count, 0, 0))
        {
            DWORD bytes_transferred;
            ULONG_PTR completion_key;
            LPOVERLAPPED overlapped;
            BOOL is_success = GetQueuedCompletionStatus(
                pf->iocp,
                &bytes_transferred,
                &completion_key,
                &overlapped,
                INFINITE
            );

            if (NULL != overlapped)
            {
                InterlockedDecrement(&pf->post_count);
                pf_io_context_t* context = (pf_io_context_t*)overlapped;
                session_release(context->session);
            }
        }
    }

    return 0;
}

// 地址转换
static int parse_ip_addr(const char* ip, unsigned short port, pf_sockaddr_t* addr)
{
    // 尝试IPv4
    if (1 == inet_pton(AF_INET, ip, &addr->sin4.sin_addr))
    {
        addr->sin4.sin_family = AF_INET;
        addr->sin4.sin_port = htons(port);
        return 0;
    }

    // 尝试IPv6
    if (1 == inet_pton(AF_INET6, ip, &addr->sin6.sin6_addr))
    {
        addr->sin6.sin6_family = AF_INET6;
        addr->sin6.sin6_port = htons(port);
        return 0;
    }

    return -1;
}

// 投递Accept
static int post_accept(pf_tcp_t* pf)
{
    DWORD bytes_received;
    pf_session_t* session = session_create(pf);
    if (NULL == session)
    {
        goto ERROR_1;
    }

    InterlockedIncrement(&pf->post_count);
    session->src_io.operation_type = PF_IO_ACCEPT;
    if (FALSE == pf->acceptex(pf->listen_sock, session->src_sock,
        session->src_addr_buffer, 0, sizeof(pf_sockaddr_t) + 16, sizeof(pf_sockaddr_t) + 16,
        &bytes_received, &session->src_io.overlapped))
    {
        if (WSA_IO_PENDING != WSAGetLastError())
        {
            InterlockedDecrement(&pf->post_count);
            goto ERROR_2;
        }
    }

    return 0;

ERROR_2:
    session_release(session);
ERROR_1:
    return -1;
}

// 投递Connect
static int post_connect(pf_tcp_t* pf, pf_session_t* session)
{
    DWORD bytes_sent;
    DWORD dst_addr_len, out_addr_len;
    session_addref(session);

    if (AF_INET == pf->dst_addr.addr.sa_family)
    {
        dst_addr_len = sizeof(pf->dst_addr.sin4);
    }
    else if (AF_INET6 == pf->dst_addr.addr.sa_family)
    {
        dst_addr_len = sizeof(pf->dst_addr.sin6);
    }
    else
    {
        goto ERROR_1;
    }

    if (AF_INET == pf->out_addr.addr.sa_family)
    {
        out_addr_len = sizeof(pf->out_addr.sin4);
    }
    else if (AF_INET6 == pf->out_addr.addr.sa_family)
    {
        out_addr_len = sizeof(pf->out_addr.sin6);
    }
    else
    {
        goto ERROR_1;
    }

    if (SOCKET_ERROR == bind(session->dst_sock,
        &pf->out_addr.addr, out_addr_len))
    {
        goto ERROR_1;
    }

    InterlockedIncrement(&pf->post_count);
    session->dst_io.operation_type = PF_IO_CONNECT;
    if (FALSE == pf->connectex(session->dst_sock, &pf->dst_addr.addr, dst_addr_len,
        NULL, 0, &bytes_sent, &session->dst_io.overlapped))
    {
        if (WSA_IO_PENDING != WSAGetLastError())
        {
            InterlockedDecrement(&pf->post_count);
            goto ERROR_1;
        }
    }

    return 0;

ERROR_1:
    session_release(session);
    return -1;
}

// 投递Timeout
static int post_timeout(pf_tcp_t* pf, pf_session_t* session)
{
    session_addref(session);
    if (PF_SESSION_TIMEOUT_NONE != InterlockedCompareExchange(&session->timeout_status, PF_SESSION_TIMEOUT_START, PF_SESSION_TIMEOUT_NONE))
    {
        session_release(session);
        return -1;
    }

    InterlockedIncrement(&pf->post_count);
    session->tmo_io.operation_type = PF_IO_TIMEOUT;
    if (FALSE == ChangeTimerQueueTimer(pf->timer_queue, session->timeout_timer, pf->config.timeout_ms, 0))
    {
        InterlockedExchange(&session->timeout_status, PF_SESSION_TIMEOUT_NONE);
        InterlockedDecrement(&pf->post_count);
        session_release(session);
        return -1;
    }

    return 0;
}

// 投递Recv
static int post_recv(pf_tcp_t* pf, pf_session_t* session, pf_io_type_t type)
{
    DWORD flags = 0;
    WSABUF* buffer = NULL;
    pf_io_context_t* context = NULL;
    SOCKET sock = INVALID_SOCKET;
    session_addref(session);

    if (PF_IO_SRC_READ == type)
    {
        sock = session->src_sock;
        context = &session->src_io;
        buffer = &session->src_wsa_buffer;
        buffer->buf = session->src_data_buffer;
        buffer->len = sizeof(session->src_data_buffer);
    }
    else if (PF_IO_DST_READ == type)
    {
        sock = session->dst_sock;
        context = &session->dst_io;
        buffer = &session->dst_wsa_buffer;
        buffer->buf = session->dst_data_buffer;
        buffer->len = sizeof(session->dst_data_buffer);
    }
    else
    {
        goto ERROR_1;
    }

    context->operation_type = type;
    InterlockedIncrement(&pf->post_count);
    memset(&context->overlapped, 0, sizeof(OVERLAPPED));
    if (SOCKET_ERROR == WSARecv(sock, buffer, 1, NULL, &flags, &context->overlapped, NULL))
    {
        if (WSA_IO_PENDING != WSAGetLastError())
        {
            InterlockedDecrement(&pf->post_count);
            goto ERROR_1;
        }
    }

    return 0;

ERROR_1:
    session_release(session);
    return -1;
}

// 投递Send
static int post_send(pf_tcp_t* pf, pf_session_t* session, pf_io_type_t type, int bytes_transferred, int offset)
{
    WSABUF* buffer = NULL;
    pf_io_context_t* context = NULL;
    SOCKET sock = INVALID_SOCKET;
    session_addref(session);

    if (PF_IO_SRC_WRITE == type)
    {
        sock = session->src_sock;
        context = &session->dst_io;
        buffer = &session->dst_wsa_buffer;
        if (offset > 0)
        {
            buffer->buf += offset;
            buffer->len -= bytes_transferred;
        }
        else
        {
            buffer->buf = session->dst_data_buffer;
            buffer->len = bytes_transferred;
        }
    }
    else if (PF_IO_DST_WRITE == type)
    {
        sock = session->dst_sock;
        context = &session->src_io;
        buffer = &session->src_wsa_buffer;
        if (offset > 0)
        {
            buffer->buf += offset;
            buffer->len -= bytes_transferred;
        }
        else
        {
            buffer->buf = session->src_data_buffer;
            buffer->len = bytes_transferred;
        }
    }
    else
    {
        goto ERROR_1;
    }

    context->operation_type = type;
    InterlockedIncrement(&pf->post_count);
    memset(&context->overlapped, 0, sizeof(OVERLAPPED));
    if (SOCKET_ERROR == WSASend(sock, buffer, 1, NULL, 0, &context->overlapped, NULL))
    {
        if (WSA_IO_PENDING != WSAGetLastError())
        {
            InterlockedDecrement(&pf->post_count);
            goto ERROR_1;
        }
    }

    return 0;

ERROR_1:
    session_release(session);
    return -1;
}

// 创建会话
static pf_session_t* session_create(pf_tcp_t* pf)
{
    pf_session_t* session = (pf_session_t*)malloc(sizeof(pf_session_t));
    if (NULL == session)
    {
        goto ERROR_1;
    }

    // 初始化
    session->ref = 1;
    session->io_ref = 0;
    session->close_flags = PF_SESSION_NONE;
    session->timeout_status = PF_SESSION_TIMEOUT_NONE;
    session->pf = pf;

    session->src_local_addr = NULL;
    session->src_remote_addr = NULL;
    session->src_wsa_buffer.buf = NULL;
    session->src_wsa_buffer.len = 0;

    session->dst_local_addr = NULL;
    session->dst_remote_addr = NULL;
    session->dst_wsa_buffer.buf = NULL;
    session->dst_wsa_buffer.len = 0;

    memset(&session->src_io, 0, sizeof(pf_io_context_t));
    memset(&session->dst_io, 0, sizeof(pf_io_context_t));
    memset(&session->tmo_io, 0, sizeof(pf_io_context_t));
    session->src_io.session = session;
    session->dst_io.session = session;
    session->tmo_io.session = session;

    // 创建源套接字
    session->src_sock = WSASocketW(pf->src_addr.addr.sa_family,
        SOCK_STREAM, IPPROTO_TCP, NULL, 0, WSA_FLAG_OVERLAPPED);
    if (INVALID_SOCKET == session->src_sock)
    {
        goto ERROR_2;
    }

    // 将源套接字与IOCP关联
    if (NULL == CreateIoCompletionPort((HANDLE)session->src_sock, pf->iocp, (ULONG_PTR)pf, 0))
    {
        goto ERROR_3;
    }

    // 创建目标套接字
    session->dst_sock = WSASocketW(pf->dst_addr.addr.sa_family,
        SOCK_STREAM, IPPROTO_TCP, NULL, 0, WSA_FLAG_OVERLAPPED);
    if (INVALID_SOCKET == session->dst_sock)
    {
        goto ERROR_3;
    }

    // 将连接套接字与IOCP关联
    if (NULL == CreateIoCompletionPort((HANDLE)session->dst_sock, pf->iocp, (ULONG_PTR)pf, 0))
    {
        goto ERROR_4;
    }

    // 超时定时器
    if (FALSE == CreateTimerQueueTimer(&session->timeout_timer, pf->timer_queue,
        handle_timer_callback, session, INFINITE, 0, WT_EXECUTEONLYONCE))
    {
        goto ERROR_4;
    }

    // 获取唯一ID
    session->session_id = table_acquire_id(pf->session_table);
    if (-1 == session->session_id)
    {
        goto ERROR_5;
    }

    // 写入会话指针
    if (-1 == table_set(pf->session_table, session->session_id, (uintptr_t)session))
    {
        goto ERROR_6;
    }

    return session;

ERROR_6:
    table_free_id(pf->session_table, session->session_id);
ERROR_5:
    (void)DeleteTimerQueueTimer(session->pf->timer_queue,
        session->timeout_timer, INVALID_HANDLE_VALUE);
ERROR_4:
    closesocket(session->dst_sock);
ERROR_3:
    closesocket(session->src_sock);
ERROR_2:
    free(session);
ERROR_1:
    return NULL;
}

// 增加会话引用
static void session_addref(pf_session_t* session)
{
    InterlockedIncrement(&session->ref);
}

// 增加会话线程引用
static void session_io_addref(pf_session_t* session)
{
    InterlockedIncrement(&session->io_ref);
}

// 减少会话线程引用
static void session_io_release(pf_session_t* session)
{
    LONG flags = 0;
    if (0 == InterlockedDecrement(&session->io_ref))
    {
        flags = InterlockedCompareExchange(&session->close_flags, 0, 0);
        if (0 == flags)
        {
            return;
        }

        if (PF_SESSION_CLOSE == (flags & PF_SESSION_CLOSE))
        {
            // 强制关闭
            if (INVALID_SOCKET != session->src_sock)
            {
                closesocket(session->src_sock);
                session->src_sock = INVALID_SOCKET;
            }
            if (INVALID_SOCKET != session->dst_sock)
            {
                closesocket(session->dst_sock);
                session->dst_sock = INVALID_SOCKET;
            }
        }
        else if ((PF_SESSION_SRC_CLOSE == (flags & PF_SESSION_SRC_CLOSE)) && (PF_SESSION_DST_CLOSE == (flags & PF_SESSION_DST_CLOSE)))
        {
            // 源与目标已关闭
            if (INVALID_SOCKET != session->dst_sock)
            {
                shutdown(session->dst_sock, SD_SEND);
            }
            if (INVALID_SOCKET != session->src_sock)
            {
                shutdown(session->src_sock, SD_SEND);
            }
        }
        else if (PF_SESSION_SRC_CLOSE == (flags & PF_SESSION_SRC_CLOSE))
        {
            // 源已关闭，半关闭目标连接的发送
            if (INVALID_SOCKET != session->dst_sock)
            {
                shutdown(session->dst_sock, SD_SEND);
            }
        }
        else if (PF_SESSION_DST_CLOSE == (flags & PF_SESSION_DST_CLOSE))
        {
            // 目标已关闭，半关闭源连接的发送
            if (INVALID_SOCKET != session->src_sock)
            {
                shutdown(session->src_sock, SD_SEND);
            }
        }
    }
}

// 会话请求关闭
static void session_request_close(pf_session_t* session, pf_session_close_flag_t flag)
{
    InterlockedOr(&session->close_flags, flag);
}

// 会话取消超时
static void session_cancel_timeout(pf_session_t* session)
{
    pf_tcp_t* pf = session->pf;
    if (PF_SESSION_TIMEOUT_START == InterlockedCompareExchange(&session->timeout_status, PF_SESSION_TIMEOUT_END, PF_SESSION_TIMEOUT_START))
    {
        PostQueuedCompletionStatus(pf->iocp, 0, (ULONG_PTR)pf, &session->tmo_io.overlapped);
        (void)ChangeTimerQueueTimer(pf->timer_queue, session->timeout_timer, INFINITE, 0);
        InterlockedExchange(&session->timeout_status, PF_SESSION_TIMEOUT_NONE);
    }
}

// 释放会话
static void session_release(pf_session_t* session)
{
    if (0 == InterlockedDecrement(&session->ref))
    {
        session_free(session);
    }
}

// 强制释放
static void session_free(pf_session_t* session)
{
    table_free_id(session->pf->session_table, session->session_id);

    if (NULL != session->timeout_timer)
    {
        (void)DeleteTimerQueueTimer(session->pf->timer_queue,
            session->timeout_timer, INVALID_HANDLE_VALUE);
        session->timeout_timer = NULL;
    }

    if (INVALID_SOCKET != session->src_sock)
    {
        closesocket(session->src_sock);
        session->src_sock = INVALID_SOCKET;
    }

    if (INVALID_SOCKET != session->dst_sock)
    {
        closesocket(session->dst_sock);
        session->dst_sock = INVALID_SOCKET;
    }

    free(session);
}

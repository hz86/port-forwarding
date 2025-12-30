#ifndef POOL_H
#define POOL_H

#include <stdint.h>
#include <Windows.h>

#ifdef __cplusplus
extern "C" {
#endif

    // 内存池句柄
    typedef struct pool_t pool_t;

    // 内存池空时触发申请操作，内存需 pool_mem_alloc 申请
    typedef void* (*pool_alloc_cb)(pool_t* pool, void* user_data);

    // 内存池回收时触发重置操作，重置失败返回NULL
    typedef void* (*pool_reset_cb)(pool_t* pool, void* user_data, void* mem);

    // 内存池关闭时触发
    typedef void (*pool_free_cb)(pool_t* pool, void* user_data, void* mem);

    // 创建内存池，min_size: 最小数量，alignment: 内存对齐
    pool_t* pool_create(void* user_data, size_t min_size, size_t alignment, 
        pool_alloc_cb alloc, pool_reset_cb reset, pool_free_cb free
    );

    // 关闭内存池
    void pool_close(pool_t* pool);

    // 申请内存
    void* pool_mem_alloc(pool_t* pool, size_t size);

    // 释放内存
    void pool_mem_free(pool_t* pool, void* mem);

    // 内存池取出
    void* pool_get(pool_t* pool);

    // 放回内存池
    void pool_put(pool_t* pool, void* mem);

#ifdef __cplusplus
}
#endif

#endif // POOL_H

#define _CRT_SECURE_NO_WARNINGS

#include <string.h>
#include <stdlib.h>
#include "pool.h"

#ifdef _WIN64

#ifndef InterlockedOrSizeT
#define InterlockedOrSizeT(a, b) InterlockedOr64((LONG64 *)a, b)
#endif

#ifndef InterlockedIncrementSizeT
#define InterlockedIncrementSizeT(a) InterlockedIncrement64((LONG64 *)a)
#endif

#ifndef InterlockedDecrementSizeT
#define InterlockedDecrementSizeT(a) InterlockedDecrement64((LONG64 *)a)
#endif

#elif _WIN32

#ifndef InterlockedOrSizeT
#define InterlockedOrSizeT(a, b) InterlockedOr((LONG *)a, b)
#endif

#ifndef InterlockedIncrementSizeT
#define InterlockedIncrementSizeT(a) InterlockedIncrement((LONG *)a)
#endif

#ifndef InterlockedDecrementSizeT
#define InterlockedDecrementSizeT(a) InterlockedDecrement((LONG *)a)
#endif

#endif

// 内存池
typedef struct pool_t {
    SLIST_HEADER head;
    volatile size_t pool_size;
    volatile size_t active_count;
    volatile LONG cleanup;
    size_t min_size;
    size_t node_size;
    size_t alignment;
    pool_alloc_cb alloc;
    pool_reset_cb reset;
    pool_free_cb free;
    void* user_data;
} pool_t;

// 内存池节点
typedef struct pool_node_t {
    SLIST_ENTRY entry;
} pool_node_t;

// 创建内存池
pool_t* pool_create(void* user_data, size_t min_size, size_t alignment, pool_alloc_cb alloc, pool_reset_cb reset, pool_free_cb free)
{
    pool_t* pool = (pool_t*)_aligned_malloc(sizeof(pool_t), MEMORY_ALLOCATION_ALIGNMENT);
    if (NULL == pool)
    {
        return NULL;
    }

    if (0 == alignment)
    {
        alignment = sizeof(void*);
    }

    InitializeSListHead(&pool->head);
    pool->node_size = ((sizeof(SLIST_ENTRY) + alignment - 1) & ~(alignment - 1));
    pool->alignment = alignment <= MEMORY_ALLOCATION_ALIGNMENT ? MEMORY_ALLOCATION_ALIGNMENT : alignment;
    pool->min_size = min_size;
    pool->user_data = user_data;
    pool->alloc = alloc;
    pool->reset = reset;
    pool->free = free;
    pool->active_count = 0;
    pool->pool_size = 0;
    pool->cleanup = 0;
    return pool;
}

// 关闭内存池
void pool_close(pool_t* pool)
{
    for (;;)
    {
        pool_node_t* node = (pool_node_t*)InterlockedPopEntrySList(&pool->head);
        if (NULL != node)
        {
            void* mem = (void*)((char*)node + pool->node_size);
            pool->free(pool, pool->user_data, mem);
        }
        else
        {
            break;
        }
    }

    _aligned_free(pool);
}

// 申请内存
void* pool_mem_alloc(pool_t* pool, size_t size)
{
    void* mem = _aligned_malloc(pool->node_size + size, pool->alignment);
    if (NULL != mem)
    {
        return (void*)((char*)mem + pool->node_size);
    }
    
    return NULL;
}

// 释放内存
void pool_mem_free(pool_t* pool, void* mem)
{
    if (NULL != mem)
    {
        mem = (void*)((char*)mem - pool->node_size);
        _aligned_free(mem);
    }
}

// 内存池取出
void* pool_get(pool_t* pool)
{
    void* mem = NULL;
    pool_node_t* node = (pool_node_t*)InterlockedPopEntrySList(&pool->head);
    if (NULL != node)
    {
        InterlockedDecrementSizeT(&pool->pool_size);
        InterlockedIncrementSizeT(&pool->active_count);
        mem = (void*)((char*)node + pool->node_size);
        return mem;
    }
    
    mem = pool->alloc(pool, pool->user_data);
    if (NULL != mem)
    {
        InterlockedIncrementSizeT(&pool->active_count);
        return mem;
    }
    
    return NULL;
}

// 放回内存池
void pool_put(pool_t* pool, void* mem)
{
    size_t current_size = InterlockedOrSizeT(&pool->pool_size, 0);
    size_t current_active = InterlockedDecrementSizeT(&pool->active_count);
    size_t max_allowed = current_active / 5;
    if (max_allowed < pool->min_size)
    {
        max_allowed = pool->min_size;
    }

    if (current_size > max_allowed)
    {
        pool->free(pool, pool->user_data, mem);
        if (0 == InterlockedCompareExchange(&pool->cleanup, 1, 0))
        {
            size_t count = (current_size - max_allowed) / 10;
            for (size_t i = 0; i < count; i++)
            {
                pool_node_t* node = (pool_node_t*)InterlockedPopEntrySList(&pool->head);
                if (NULL != node)
                {
                    InterlockedDecrementSizeT(&pool->pool_size);
                    void* gc_mem = (void*)((char*)node + pool->node_size);
                    pool->free(pool, pool->user_data, gc_mem);
                }
                else
                {
                    break;
                }
            }

            InterlockedExchange(&pool->cleanup, 0);
        }

        return;
    }

    void* new_mem = pool->reset(pool, pool->user_data, mem);
    if (NULL != new_mem)
    {
        InterlockedIncrementSizeT(&pool->pool_size);
        pool_node_t* node = (pool_node_t*)((char*)new_mem - pool->node_size);
        InterlockedPushEntrySList(&pool->head, (SLIST_ENTRY*)node);
        return;
    }

    pool->free(pool, pool->user_data, mem);
}

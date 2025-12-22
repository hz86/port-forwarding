#define _CRT_SECURE_NO_WARNINGS

#include <string.h>
#include <stdlib.h>
#include "pool.h"

// 内存池
typedef struct pool_t {
    SLIST_HEADER head;
    volatile size_t size;
    pool_alloc_t alloc;
    pool_reset_t reset;
    pool_free_t free;
    void* user_data;
    size_t max_size;
    size_t node_size;
    size_t alignment;
} pool_t;

// 内存池节点
typedef struct pool_node_t {
    SLIST_ENTRY entry;
} pool_node_t;

// 创建内存池
pool_t* pool_create(void* user_data, size_t max_size, size_t alignment, pool_alloc_t alloc, pool_reset_t reset, pool_free_t free)
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
    pool->max_size = max_size;
    pool->user_data = user_data;
    pool->alloc = alloc;
    pool->reset = reset;
    pool->free = free;
    pool->size = 0;
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
        InterlockedDecrementSizeT(&pool->size);
        mem = (void*)((char*)node + pool->node_size);
        return mem;
    }
    
    mem = pool->alloc(pool, pool->user_data);
    if (NULL != mem)
    {
        return mem;
    }
    
    return NULL;
}

// 放回内存池
void pool_put(pool_t* pool, void* mem)
{
    size_t new_size = InterlockedIncrementSizeT(&pool->size);
    if (new_size > pool->max_size)
    {
        InterlockedDecrementSizeT(&pool->size);
        pool->free(pool, pool->user_data, mem);
        return;
    }

    void* new_mem = pool->reset(pool, pool->user_data, mem);
    if (NULL != new_mem)
    {
        pool_node_t* node = (pool_node_t*)((char*)new_mem - pool->node_size);
        InterlockedPushEntrySList(&pool->head, (SLIST_ENTRY*)node);
        return;
    }

    InterlockedDecrementSizeT(&pool->size);
    pool->free(pool, pool->user_data, mem);
}

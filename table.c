#define _CRT_SECURE_NO_WARNINGS

#include <string.h>
#include <stdlib.h>
#include <Windows.h>
#include "table.h"

#ifndef SSIZE_MAX
#define SSIZE_MAX ((ssize_t)(SIZE_MAX >> 1))
#endif

// 表成员
typedef struct table_entry_t {
    union {
        uintptr_t data;       // 活跃时：存储数据
        ssize_t next_free_id; // 空闲时：存储下一个空闲ID (-1 表示链表末尾)
    };
    short is_alloc;
} table_entry_t;

// 表结构
typedef struct table_t {
    volatile LONG lock;             // 自旋锁
    volatile ssize_t next_alloc_id; // 下一个分配的ID
    ssize_t free_head_id;           // 空闲链表头ID (-1 表示无回收ID)
    size_t capacity;                // 数组的容量
    size_t block_count;             // 当前块数量
    size_t block_shift;             // 例如 1024 是 2^10，这里存 10
    size_t block_mask;              // 例如 1023 (0x3FF)
    size_t block_size;              // 每块可容纳的成员数
    table_entry_t** array;          // 存放各块指针的数组
} table_t;

// 表计算log2
static inline size_t table_log2(size_t x)
{
    size_t v = 0;
    while (x > 1)
    {
        x >>= 1;
        v++;
    }

    return v;
}

// 表计算是否2的幂
static inline int table_is_power_of_two(size_t n) {
    return (n > 0) && ((n & (n - 1)) == 0);
}

// 表加锁
static inline void table_lock(table_t* table)
{
    unsigned int spin_count = 0;
    while (0 != InterlockedCompareExchange(&table->lock, 1, 0))
    {
        if (spin_count++ < 8000)
        {
            // 短暂让出CPU
            YieldProcessor();
        }
        else
        {
            // 让出CPU
            spin_count = 0;
            Sleep(0);
        }
    }
}

// 表解锁
static inline void table_unlock(table_t* table)
{
    InterlockedExchange(&table->lock, 0);
}

// 表取成员指针
static inline table_entry_t* table_get_entry(table_t* table, ssize_t id)
{
    // 优化 offset = id & entries_per_block
    // 优化 block_id = id / entries_per_block
    size_t block_id = (size_t)id >> table->block_shift;
    size_t offset = (size_t)id & table->block_mask;
    return &table->array[block_id][offset];
}

// 创建表 block_size 必须是 2的幂
table_t* table_create(size_t capacity, size_t block_size)
{
    table_t* table;

    if (0 == capacity || 0 == block_size)
    {
        goto ERROR_1;
    }

    if (0 == table_is_power_of_two(block_size))
    {
        goto ERROR_1;
    }

    table = (table_t*)malloc(sizeof(table_t));
    if (NULL == table)
    {
        goto ERROR_1;
    }

    table->capacity = capacity;
    table->array = (table_entry_t**)malloc(table->capacity * sizeof(table_entry_t*));
    if (NULL == table->array)
    {
        goto ERROR_2;
    }

    table->block_count = 1;
    table->block_size = block_size;
    for (size_t i = 0; i < table->block_count; i++)
    {
        table->array[i] = (table_entry_t*)malloc(block_size * sizeof(table_entry_t));
        if (NULL == table->array[i])
        {
            for (size_t j = 0; j < i; j++)
                free(table->array[j]);
            goto ERROR_3;
        }
    }

    table->block_mask = block_size - 1;
    table->block_shift = table_log2(block_size);
    table->free_head_id = -1;
    table->next_alloc_id = 0;
    table->lock = 0;
    return table;

ERROR_3:
    free(table->array);
ERROR_2:
    free(table);
ERROR_1:
    return NULL;
}

// 关闭表
void table_close(table_t* table)
{
    for (size_t i = 0; i < table->block_count; i++)
    {
        free(table->array[i]);
    }

    free(table->array);
    free(table);
}

// 表获取最大ID
ssize_t table_max_id(table_t* table)
{
    return table->next_alloc_id - 1;
}

// 表获取唯一ID
ssize_t table_acquire_id(table_t* table)
{
    for (;;)
    {
        ssize_t id;
        table_lock(table);

        // 取出回收的ID
        if (table->free_head_id >= 0)
        {
            id = table->free_head_id;
            table_entry_t* entry = table_get_entry(table, id);
            table->free_head_id = entry->next_free_id;
            entry->is_alloc = 1;
            entry->data = 0;
            table_unlock(table);
            return id;
        }

        // 是否到达极限值
        if (table->next_alloc_id >= SSIZE_MAX)
        {
            table_unlock(table);
            return -1;
        }

        // 分配新ID
        size_t block_id = (size_t)table->next_alloc_id >> table->block_shift;
        if (block_id < table->block_count)
        {
            id = table->next_alloc_id++;
            table_entry_t* entry = table_get_entry(table, id);
            entry->is_alloc = 1;
            entry->data = 0;
            table_unlock(table);
            return id;
        }

        // 直接增加块
        if (block_id < table->capacity)
        {
            // 解锁,降低锁的粒度
            size_t old_block_count = table->block_count;
            table_unlock(table);

            // 分配新块内存
            table_entry_t* new_block = (table_entry_t*)malloc(table->block_size * sizeof(table_entry_t));
            if (NULL == new_block)
            {
                return -1;
            }

            // 加锁
            table_lock(table);
            if (old_block_count != table->block_count)
            {
                // 被抢占了，重试
                table_unlock(table);
                free(new_block);
                continue;
            }

            // 添加
            table->array[block_id] = new_block;
            table->block_count++;
            table_unlock(table);
            continue;
        }

        // 需要扩容数组
        size_t new_capacity = table->capacity * 2;
        if (new_capacity < table->capacity)
        {
            table_unlock(table);
            return -1;
        }

        // 解锁,降低锁的粒度
        size_t old_capacity = table->capacity;
        table_entry_t** old_array = table->array;
        table_unlock(table);

        // 分配块数组内存
        table_entry_t** new_array = (table_entry_t**)malloc(new_capacity * sizeof(table_entry_t*));
        if (NULL == new_array)
        {
            return -1;
        }

        // 加锁
        table_lock(table);
        if (old_capacity != table->capacity || old_array != table->array)
        {
            // 被抢占了，重试
            table_unlock(table);
            free(new_array);
            continue;
        }

        // 更新指针
        memcpy(new_array, table->array, old_capacity * sizeof(table_entry_t*));
        table->capacity = new_capacity;
        table->array = new_array;
        table_unlock(table);
        free(old_array);
        continue;
    }
}

// 表删除ID
int table_free_id(table_t* table, ssize_t id)
{
    if (id < 0 || id >= table->next_alloc_id)
    {
        return -1;
    }

    int result = -1;
    table_lock(table);

    table_entry_t* entry = table_get_entry(table, id);
    if (0 != entry->is_alloc)
    {
        entry->is_alloc = 0;
        entry->next_free_id = table->free_head_id;
        table->free_head_id = id;
        result = 0;
    }
    
    table_unlock(table);
    return result;
}

// 表设置数据
int table_set(table_t* table, ssize_t id, uintptr_t data)
{
    if (id < 0 || id >= table->next_alloc_id)
    {
        return -1;
    }

    int result = -1;
    table_lock(table);

    table_entry_t* entry = table_get_entry(table, id);
    if (0 != entry->is_alloc)
    {
        entry->data = data;
        result = 0;
    }
    
    table_unlock(table);
    return result;
}

// 表获取数据
int table_get(table_t* table, ssize_t id, uintptr_t* data)
{
    if (id < 0 || id >= table->next_alloc_id)
    {
        return -1;
    }

    int result = -1;
    table_lock(table);

    table_entry_t* entry = table_get_entry(table, id);
    if (0 != entry->is_alloc)
    {
        *data = entry->data;
        result = 0;
    }
    
    table_unlock(table);
    return result;
}

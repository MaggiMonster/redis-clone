#include <string>     // std::string  (your keys and values)
#include <vector>     // std::vector  (the bucket array)
#include <functional> // std::hash    (to hash string keys into bucket indices)
#include <iostream>   // std::cout    (printing test results in main)
#include <cstddef>    // size_t       (sizes / indices)

struct Entry
{
    std::string key;
    std::string value;
    Entry *next;
};
struct Table
{
    std::vector<Entry *> buckets;
    size_t count;
};
struct Dict
{
    Table ht[2];
    long rehash_idx;
    Dict()
    {
        ht[0].buckets.assign(8, nullptr);
        ht[0].count = 0;
        rehash_idx = -1;
    }
    void rehash_step()
    {
        if (rehash_idx == -1)
            return;
        Entry *cur = ht[0].buckets[rehash_idx];
        while (cur != nullptr)
        {
            Entry *next = cur->next;
            size_t i = std::hash<std::string>{}(cur->key) % ht[1].buckets.size();
            cur->next = ht[1].buckets[i];
            ht[1].buckets[i] = cur;
            ht[1].count++;
            ht[0].count--;
            cur = next;
        }
        ht[0].buckets[rehash_idx] = nullptr;
        rehash_idx++;
        if (rehash_idx == (long)ht[0].buckets.size())
        {
            ht[0] = ht[1];
            ht[1] = Table{};
            rehash_idx = -1;
        }
    }
    Entry *get(const std::string &key)
    {
        rehash_step();
        size_t index = std::hash<std::string>{}(key) % ht[0].buckets.size();
        Entry *cur = ht[0].buckets[index];
        while (cur != nullptr)
        {
            if (cur->key == key)
                return cur;
            cur = cur->next;
        }
        if (rehash_idx != -1)
        {
            size_t idx = std::hash<std::string>{}(key) % ht[1].buckets.size();
            Entry *current = ht[1].buckets[idx];
            while (current != nullptr)
            {
                if (current->key == key)
                    return current;
                current = current->next;
            }
        }
        return nullptr;
    }
    void set(const std::string &key, const std::string &value)
    {
        rehash_step();
        size_t index = std::hash<std::string>{}(key) % ht[0].buckets.size();
        Entry *cur = ht[0].buckets[index];
        while (cur != nullptr)
        {
            if (cur->key == key)
            {
                cur->value = value;
                return;
            }
            cur = cur->next;
        }
        if (rehash_idx != -1)
        {
            size_t index = std::hash<std::string>{}(key) % ht[1].buckets.size();
            Entry *cur = ht[1].buckets[index];
            while (cur != nullptr)
            {
                if (cur->key == key)
                {
                    cur->value = value;
                    return;
                }
                cur = cur->next;
            }
        }
        Table&t = (rehash_idx==-1)?ht[0]:ht[1];
        size_t idx=std::hash<std::string>{}(key)%t.buckets.size();
        Entry *node = new Entry({key, value, t.buckets[idx]});
        t.buckets[idx] = node;
        t.count++;

        if (rehash_idx == -1 && ht[0].count >= ht[0].buckets.size())
        {
            ht[1].buckets.assign(ht[0].buckets.size() * 2, nullptr);
            ht[1].count = 0;
            rehash_idx = 0;
        }
        return;
    }
    void del(const std::string &key)
    {
        rehash_step();
        size_t index = std::hash<std::string>{}(key) % ht[0].buckets.size();
        Entry *cur = ht[0].buckets[index];
        Entry *prev = nullptr;
        while (cur != nullptr)
        {
            if (cur->key == key)
            {
                if (prev == nullptr)
                {
                    ht[0].buckets[index] = ht[0].buckets[index]->next;
                    delete cur;
                    ht[0].count--;
                    return;
                }
                else
                {
                    prev->next = cur->next;
                    delete cur;
                    ht[0].count--;
                    return;
                }
            }
            prev = cur;
            cur = cur->next;
        }
        if (rehash_idx != -1)
        {
            size_t index = std::hash<std::string>{}(key) % ht[1].buckets.size();
            Entry *cur = ht[1].buckets[index];
            Entry *prev = nullptr;
            while (cur != nullptr)
            {
                if (cur->key == key)
                {
                    if (prev == nullptr)
                    {
                        ht[1].buckets[index] = ht[1].buckets[index]->next;
                        delete cur;
                        ht[1].count--;
                        return;
                    }
                    else
                    {
                        prev->next = cur->next;
                        delete cur;
                        ht[1].count--;
                        return;
                    }
                }
                prev = cur;
                cur = cur->next;
            }
        }
    }
};

int main() {
    Dict d;

    // insert 50 keys — this crosses the load factor and triggers a rehash
    for (int i = 0; i < 50; i++) {
        d.set("key" + std::to_string(i), "val" + std::to_string(i));
    }

    // read every key back — all 50 must be found with the right value
    int found = 0;
    for (int i = 0; i < 50; i++) {
        Entry* e = d.get("key" + std::to_string(i));
        if (e && e->value == "val" + std::to_string(i)) {
            found++;
        } else {
            std::cout << "MISSING: key" << i << "\n";
        }
    }
    std::cout << "found " << found << " / 50\n";

    // sanity: delete a few, confirm they're gone and the rest survive
    d.del("key0");
    d.del("key25");
    d.del("key49");
    std::cout << "key0  -> " << (d.get("key0")  ? "still here (BUG)" : "deleted ok") << "\n";
    std::cout << "key25 -> " << (d.get("key25") ? "still here (BUG)" : "deleted ok") << "\n";
    std::cout << "key10 -> " << (d.get("key10") ? "found ok" : "GONE (BUG)") << "\n";

    return 0;
}
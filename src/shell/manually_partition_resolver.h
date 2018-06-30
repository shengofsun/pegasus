#pragma once

#include <cstdio>

#include <dsn/utility/factory_store.h>
#include <dsn/utility/synchronize.h>
#include <dsn/c/api_utilities.h>
#include <dsn/tool-api/partition_resolver.h>

namespace pegasus {

class manually_partition_resolver : public dsn::dist::partition_resolver
{
public:
    manually_partition_resolver(dsn::rpc_address meta_server, const char *app_path)
        : partition_resolver(meta_server, app_path)
    {
        printf("create partition resolver with meta(%s), app_path(%s)\n",
               meta_server.to_string(),
               app_path);
    }
    virtual ~manually_partition_resolver() override {}

public:
    virtual void
    resolve(uint64_t partition_hash,
            std::function<void(dsn::dist::partition_resolver::resolve_result &&)> &&callback,
            int /*timeout_ms*/) override
    {
        dsn::dist::partition_resolver::resolve_result res;

        {
            dsn::utils::auto_lock<dsn::utils::ex_lock_nr> l(s_lock);
            res.err = dsn::ERR_OK;
            res.address = s_target_addr;
            res.pid.set_app_id(s_target_app_id);
            res.pid.set_partition_index(get_partition_index(s_target_partitions, partition_hash));
        }
        callback(std::move(res));
    }

    virtual void on_access_failure(int partition_index, dsn::error_code err) override
    {
        fprintf(stderr, "access %d with %s\n", partition_index, err.to_string());
    }

    virtual int get_partition_index(int partition_count, uint64_t partition_hash) override
    {
        return partition_hash % static_cast<uint64_t>(s_target_partitions);
    }

private:
    static int32_t s_target_app_id;
    static int32_t s_target_partitions;
    static dsn::rpc_address s_target_addr;
    static dsn::utils::ex_lock_nr s_lock;

public:
    static void set_target(int32_t app_id, int32_t partition_count, dsn::rpc_address addr)
    {
        dsn::utils::auto_lock<dsn::utils::ex_lock_nr> l(s_lock);
        s_target_app_id = app_id;
        s_target_partitions = partition_count;
        s_target_addr = addr;
    }
    static void register_resolver()
    {
        dsn::utils::factory_store<dsn::dist::partition_resolver>::register_factory(
            "manually_partition_resolver",
            dsn::dist::partition_resolver::create<pegasus::manually_partition_resolver>,
            dsn::PROVIDER_TYPE_MAIN);
    }
};
}

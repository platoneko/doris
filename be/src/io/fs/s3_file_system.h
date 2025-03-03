#pragma once

#include <mutex>

#include "io/fs/remote_file_system.h"
#include "util/s3_util.h"

namespace Aws::S3 {
class S3Client;
} // namespace Aws::S3
namespace Aws::Utils::Threading {
class PooledThreadExecutor;
} // namespace Aws::Utils::Threading

namespace doris {
namespace io {

// This class is thread-safe.(Except `set_xxx` method)
class S3FileSystem final : public RemoteFileSystem {
public:
    S3FileSystem(const std::map<std::string, std::string>& properties, std::string bucket,
                 std::string prefix, ResourceId resource_id);
    ~S3FileSystem() override;

    Status create_file(const Path& path, std::unique_ptr<FileWriter>* writer) override;

    Status open_file(const Path& path, std::unique_ptr<FileReader>* reader) override;

    Status delete_file(const Path& path) override;

    Status create_directory(const Path& path) override;

    Status delete_directory(const Path& path) override;

    Status link_file(const Path& src, const Path& dest) override;

    Status exists(const Path& path, bool* res) const override;

    Status file_size(const Path& path, size_t* file_size) const override;

    Status list(const Path& path, std::vector<Path>* files) override;

    Status upload(const Path& local_path, const Path& dest_path) override;

    Status batch_upload(const std::vector<Path>& local_paths,
                        const std::vector<Path>& dest_paths) override;

    Status connect() override;

    std::shared_ptr<Aws::S3::S3Client> get_client() const {
        std::lock_guard lock(_client_mu);
        return _client;
    };

    // Guarded by external lock.
    void set_ak(std::string ak) { _properties[S3_AK] = std::move(ak); }

    // Guarded by external lock.
    void set_sk(std::string sk) { _properties[S3_SK] = std::move(sk); }

private:
    std::string get_key(const Path& path) const;

private:
    std::map<std::string, std::string> _properties;
    std::string _endpoint;
    std::string _bucket;
    std::string _prefix;

    // FIXME(cyx): We can use std::atomic<std::shared_ptr> since c++20.
    std::shared_ptr<Aws::S3::S3Client> _client;
    mutable std::mutex _client_mu;

    std::shared_ptr<Aws::Utils::Threading::PooledThreadExecutor> _executor;
};

} // namespace io
} // namespace doris

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "filesystem/local_read_stream.h"

#include "gutil/macros.h"

namespace doris {

LocalReadStream::LocalReadStream(int fd, size_t file_size) : _fd(fd), _file_size(file_size) {}

LocalReadStream::~LocalReadStream() {
    close();
}

Status LocalReadStream::read(char* to, size_t req_n, size_t* read_n) {
    RETURN_IF_ERROR(read_at(_offset, to, req_n, read_n));
    _offset += *read_n;
    return Status::OK();
}

Status LocalReadStream::read_at(size_t position, char* to, size_t req_n, size_t* read_n) {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    if (position > _file_size) {
        return Status::IOError("Position exceeds file size");
    }
    req_n = std::min(req_n, _file_size - position);
    if (req_n == 0) {
        *read_n = 0;
        return Status::OK();
    }
    ssize_t res = 0;
    RETRY_ON_EINTR(res, ::pread(_fd, to, req_n, position));
    if (-1 == res) {
        return Status::IOError("Cannot read from file");
    }
    *read_n = res;
    return Status::OK();
}

Status LocalReadStream::seek(size_t position) {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    if (position > _file_size) {
        return Status::IOError("Position exceeds file size");
    }
    _offset = position;
    return Status::OK();
}

Status LocalReadStream::tell(size_t* position) const {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    *position = _offset;
    return Status::OK();
}

Status LocalReadStream::available(size_t* n_bytes) const {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    *n_bytes = _file_size - _offset;
    return Status::OK();
}

Status LocalReadStream::close() {
    _fd = -1;
    return Status::OK();
}

} // namespace doris

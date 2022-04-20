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

#include "filesystem/local_write_stream.h"

#include <unistd.h>

namespace doris {

namespace detail {

// Retry until `put_n` bytes are written.
bool write_retry(int fd, const char* from, size_t put_n) {
    while (put_n != 0) {
        auto res = ::write(fd, from, put_n);
        if (-1 == res && errno != EINTR) {
            return false;
        }
        if (res > 0) {
            from += res;
            put_n -= res;
        }
    }
    return true;
}

} // namespace detail

LocalWriteStream::LocalWriteStream(int fd, size_t buffer_size)
        : _fd(fd), _buffer_size(buffer_size) {
    _buffer = buffer_size ? new char[buffer_size] : nullptr;
}

LocalWriteStream::~LocalWriteStream() {
    close();
    delete[] _buffer;
}

Status LocalWriteStream::write(const char* from, size_t put_n) {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    _dirty = true;

    size_t copied = std::min(put_n, buffer_remain());
    memcpy(_buffer + _buffer_used, from, copied);
    _buffer_used += copied;
    from += copied;
    put_n -= copied;
    if (put_n == 0) {
        return Status::OK();
    }
    RETURN_IF_ERROR(flush());
    // Write bytes is greater than 1/2 capacity of buffer,
    // do not copy data into buffer.
    if (put_n < _buffer_size / 2) {
        memcpy(_buffer, from, put_n);
        _buffer_used = put_n;
        return Status::OK();
    }
    if (!detail::write_retry(_fd, from, put_n)) {
        return Status::IOError("Cannot write to file");
    }
    return Status::OK();
}

Status LocalWriteStream::sync() {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    if (_dirty) {
        RETURN_IF_ERROR(flush());
        if (0 != ::fdatasync(_fd)) {
            return Status::IOError("Cannot fdatasync");
        }
        _dirty = false;
    }
    return Status::OK();
}

Status LocalWriteStream::flush() {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    if (_buffer_used) {
        if (!detail::write_retry(_fd, _buffer, _buffer_used)) {
            return Status::IOError("Cannot write to file");
        }
        _buffer_used = 0;
    }
    return Status::OK();
}

Status LocalWriteStream::close() {
    if (!closed()) {
        RETURN_IF_ERROR(sync());
        if (0 != ::close(_fd)) {
            return Status::IOError("Cannot close file");
        }
        delete[] _buffer;
        _buffer = nullptr;
        _fd = -1;
    }
    return Status::OK();
}

} // namespace doris

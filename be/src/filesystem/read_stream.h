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

#pragma once

#include "common/status.h"

namespace doris {

// The implementations may contains buffers or local caches
class ReadStream {
public:
    ReadStream() = default;
    virtual ~ReadStream() = default;

    // `read_n` is set to the number of bytes read.
    virtual Status read(char* to, size_t req_n, size_t* read_n) = 0;

    // Read at a given position, the stream offset is not changed.
    virtual Status read_at(size_t position, char* to, size_t req_n, size_t* read_n) = 0;

    // Move current position to.
    virtual Status seek(size_t position) = 0;

    virtual Status tell(size_t* position) const = 0;

    virtual Status available(size_t* n_bytes) const = 0;

    virtual Status close() = 0;

    virtual bool closed() const = 0;
};

} // namespace doris

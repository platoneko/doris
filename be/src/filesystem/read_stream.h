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

#include <stdint.h>
#include <memory>

#include "common/status.h"
#include "util/slice.h"

namespace doris {

class ReadStream {
public:
    virtual ReadStream() = default;
    virtual ~ReadStream() = default;

    // Read content to 'slice.data', 'slice.size' is the max size of this buffer.
    // Return ok when read success, and 'bytes_read' is set to size of read content
    // If reach to end of file, the eof is set to true. meanwhile 'bytes_read'
    // is set to zero.
    virtual Status read(Slice& slice, int64_t* bytes_read, bool* eof) = 0;

    // Move current position to 
    virtual Status seek(int64_t position) = 0;

    virtual Status tell(int64_t* position) = 0;

    virtual Status close() = 0;
};

} // namespace doris

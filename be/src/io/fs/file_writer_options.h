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

namespace doris {
namespace io {

// Only affects remote file writers
struct FileWriterOptions {
    bool write_file_cache = false;
    bool is_cold_data = false;
    bool sync_file_data = true;        // Whether flush data into storage system
    int64_t file_cache_expiration = 0; // Absolute time
    // Whether to create empty file if no content
    bool create_empty_file = true;
};

} // namespace io
} // namespace doris

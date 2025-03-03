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

#include "olap/merger.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"

namespace doris {
class DataDir;
class ConvertRowset {
public:
    ConvertRowset(TabletSharedPtr tablet) : _tablet(tablet) {}
    Status do_convert();

private:
    Status check_correctness(RowsetSharedPtr input_rowset, RowsetSharedPtr output_rowset,
                             const Merger::Statistics& stats);
    int64_t _get_input_num_rows_from_seg_grps(RowsetSharedPtr rowset);
    Status _modify_rowsets(RowsetSharedPtr input_rowset, RowsetSharedPtr output_rowset);

private:
    TabletSharedPtr _tablet;

    DISALLOW_COPY_AND_ASSIGN(ConvertRowset);
};
} // namespace doris
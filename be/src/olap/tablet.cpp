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

#include "olap/tablet.h"

#include <bvar/reducer.h>
#include <bvar/window.h>
#include <ctype.h>
#include <fmt/core.h>
#include <glog/logging.h>
#include <opentelemetry/common/threadlocal.h>
#include <pthread.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <stdio.h>
#include <sys/stat.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "io/fs/path.h"
#include "io/fs/remote_file_system.h"
#include "olap/base_compaction.h"
#include "olap/base_tablet.h"
#include "olap/cumulative_compaction.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/reader.h"
#include "olap/row_cursor.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/schema_change.h"
#include "olap/storage_engine.h"
#include "olap/storage_policy_mgr.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/tablet_schema.h"
#include "segment_loader.h"
#include "util/path_util.h"
#include "util/pretty_printer.h"
#include "util/scoped_cleanup.h"
#include "util/time.h"
#include "util/trace.h"

namespace doris {
using namespace ErrorCode;

using std::pair;
using std::nothrow;
using std::sort;
using std::string;
using std::vector;

DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(flush_bytes, MetricUnit::BYTES);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(flush_finish_count, MetricUnit::OPERATIONS);

bvar::Adder<uint64_t> exceed_version_limit_counter;
bvar::Window<bvar::Adder<uint64_t>> xceed_version_limit_counter_minute(
        &exceed_version_limit_counter, 60);

TabletSharedPtr Tablet::create(const TabletMetaPB& tablet_meta_pb, DataDir* data_dir) {
    auto tablet_meta = std::make_shared<TabletMeta>();
    tablet_meta->init_from_pb(tablet_meta_pb);
    auto tablet = std::make_shared<Tablet>(std::move(tablet_meta), data_dir);
    if (auto st = tablet->init(tablet_meta_pb); !st.ok()) {
        LOG(WARNING) << "failed to create tablet from binary. tablet_id="
                     << tablet_meta_pb.tablet_id() << " err=" << st;
        return nullptr;
    }
    return tablet;
}

TabletSharedPtr create(const TabletMetaSharedPtr& tablet_meta, DataDir* data_dir) {
    return std::make_shared<Tablet>(tablet_meta, data_dir);
}

Tablet::Tablet(TabletMetaSharedPtr tablet_meta, DataDir* data_dir)
        : BaseTablet(std::move(tablet_meta), data_dir),
          _is_bad(false),
          _last_cumu_compaction_failure_millis(0),
          _last_base_compaction_failure_millis(0),
          _last_cumu_compaction_success_millis(0),
          _last_base_compaction_success_millis(0),
          _cumulative_point(K_INVALID_CUMULATIVE_POINT),
          _newly_created_rowset_num(0),
          _last_checkpoint_time(0),
          _is_clone_occurred(false),
          _last_missed_version(-1),
          _last_missed_time_s(0) {
    INT_COUNTER_METRIC_REGISTER(_metric_entity, flush_bytes);
    INT_COUNTER_METRIC_REGISTER(_metric_entity, flush_finish_count);
}

Status Tablet::init(const TabletMetaPB& tablet_meta_pb) {
    Status res = Status::OK();
    VLOG_NOTICE << "begin to load tablet. tablet=" << full_name()
                << ", version_size=" << tablet_meta_pb.rs_metas_size();

#ifdef BE_TEST
    // init cumulative compaction policy by type
    _cumulative_compaction_policy =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy();
#endif

    RowsetVector rowset_vec;
    for (auto& rs_meta_pb : tablet_meta_pb.rs_metas()) {
        auto rs_meta = std::make_shared<RowsetMeta>();
        if (!rs_meta->init_from_pb(rs_meta_pb)) {
            return Status::InternalError("malformed rs_meta_pb");
        }
        RowsetSharedPtr rowset;
        RETURN_IF_ERROR(RowsetFactory::create_rowset(_schema, _tablet_path, rs_meta, &rowset));
        rowset_vec.push_back(rowset);
        _rowsets.emplace(rs_meta_pb.end_version(), std::move(rowset));
    }
    for (auto& rs_meta_pb : tablet_meta_pb.stale_rs_metas()) {
        auto rs_meta = std::make_shared<RowsetMeta>();
        if (!rs_meta->init_from_pb(rs_meta_pb)) {
            return Status::InternalError("malformed rs_meta_pb");
        }
        RowsetSharedPtr rowset;
        RETURN_IF_ERROR(RowsetFactory::create_rowset(_schema, _tablet_path, rs_meta, &rowset));
        _stale_rowsets.emplace(rs_meta_pb.end_version(), std::move(rowset));
    }
    // if !_tablet_meta->all_rs_metas()[0]->tablet_schema(),
    // that means the tablet_meta is still not upgrade to doris 1.2 version.
    // Before doris 1.2 version, rowset metas don't have tablet schema.
    // And when upgrade to doris 1.2 version,
    // all rowset metas will be set the tablet schmea from tablet meta.
    if (_rowsets.empty() || !_rowsets.begin()->second->tablet_schema()) {
        _max_version_schema = BaseTablet::tablet_schema();
    } else {
        _max_version_schema = rowset_with_max_schema_version(rowset_vec)->tablet_schema();
    }
    DCHECK(_max_version_schema);

    if (_schema->keys_type() == UNIQUE_KEYS && enable_unique_key_merge_on_write()) {
        _rowset_tree = std::make_unique<RowsetTree>();
        res = _rowset_tree->Init(rowset_vec);
    }
    return res;
}

// should save tablet meta to remote meta store
// if it's a primary replica
void Tablet::save_meta() {
    auto res = _tablet_meta->save_meta(_data_dir);
    CHECK_EQ(res, Status::OK()) << "fail to save tablet_meta. res=" << res
                                << ", root=" << _data_dir->path();
}

Status Tablet::revise_tablet_meta(const std::vector<RowsetMetaSharedPtr>& rowsets_to_clone,
                                  const std::vector<Version>& versions_to_delete) {
    LOG(INFO) << "begin to revise tablet. tablet=" << full_name()
              << ", rowsets_to_clone=" << rowsets_to_clone.size()
              << ", versions_to_delete=" << versions_to_delete.size();
    Status res = Status::OK();
    RowsetVector rs_to_delete, rs_to_add;

    for (auto& version : versions_to_delete) {
        auto it = _rowsets.find(version.second);
        DCHECK(it != _rowsets.end());
        StorageEngine::instance()->add_unused_rowset(it->second);
        rs_to_delete.push_back(it->second);
        _rowsets.erase(it);
    }

    for (auto& rs_meta : rowsets_to_clone) {
        Version version = rs_meta->version();
        RowsetSharedPtr rowset;
        res = RowsetFactory::create_rowset(_schema, _tablet_path, rs_meta, &rowset);
        if (!res.ok()) {
            LOG(WARNING) << "fail to init rowset. version=" << version;
            return res;
        }
        rs_to_add.push_back(rowset);
        _rowsets[version.second] = std::move(rowset);
    }

    if (keys_type() == UNIQUE_KEYS && enable_unique_key_merge_on_write()) {
        auto new_rowset_tree = std::make_unique<RowsetTree>();
        ModifyRowSetTree(*_rowset_tree, rs_to_delete, rs_to_add, new_rowset_tree.get());
        _rowset_tree = std::move(new_rowset_tree);
        for (auto rowset_ptr : rs_to_add) {
            RETURN_IF_ERROR(update_delete_bitmap_without_lock(rowset_ptr));
        }
    }

    do {
        // load new local tablet_meta to operate on
        TabletMetaSharedPtr new_tablet_meta(new (nothrow) TabletMeta(*_tablet_meta));
        VLOG_NOTICE << "load rowsets successfully when clone. tablet=" << full_name()
                    << ", added rowset size=" << rowsets_to_clone.size();
        // save and reload tablet_meta
        res = new_tablet_meta->save_meta(_data_dir);
        if (!res.ok()) {
            LOG(WARNING) << "failed to save new local tablet_meta when clone. res:" << res;
            break;
        }
        _tablet_meta = new_tablet_meta;
    } while (0);
    // clear stale rowset
    for (auto& [v, rs] : _stale_rowsets) {
        StorageEngine::instance()->add_unused_rowset(rs);
    }
    _stale_rowsets.clear();

    LOG(INFO) << "finish to revise tablet. res=" << res << ", "
              << "table=" << full_name();
    return res;
}

Status Tablet::add_rowset(RowsetSharedPtr rowset) {
    DCHECK(rowset != nullptr);
    std::lock_guard<std::shared_mutex> wrlock(_meta_lock);
    // the version should be not contained in any existing rowset.
    if (contains_version(rowset->version())) {
        return Status::Error<PUSH_VERSION_ALREADY_EXIST>();
    }

    _rowsets[rowset->end_version()] = rowset;

    // Update rowset tree
    if (keys_type() == UNIQUE_KEYS && enable_unique_key_merge_on_write()) {
        auto new_rowset_tree = std::make_unique<RowsetTree>();
        ModifyRowSetTree(*_rowset_tree, {}, {rowset}, new_rowset_tree.get());
        _rowset_tree = std::move(new_rowset_tree);
    }
    ++_newly_created_rowset_num;
    return Status::OK();
}

Status Tablet::modify_rowsets(std::vector<RowsetSharedPtr>& to_add,
                              std::vector<RowsetSharedPtr>& to_delete, bool check_delete) {
    // the compaction process allow to compact the single version, eg: version[4-4].
    // this kind of "single version compaction" has same "input version" and "output version".
    // which means "to_add->version()" equals to "to_delete->version()".
    // So we should delete the "to_delete" before adding the "to_add",
    // otherwise, the "to_add" will be deleted from _rs_version_map, eventually.
    //
    // And if the version of "to_add" and "to_delete" are exactly same. eg:
    // to_add:      [7-7]
    // to_delete:   [7-7]
    // In this case, we no longer need to add the rowset in "to_delete" to
    // _stale_rs_version_map, but can delete it directly.

    if (to_add.empty() && to_delete.empty()) {
        return Status::OK();
    }

    bool same_version = true;
    std::sort(to_add.begin(), to_add.end(), Rowset::comparator);
    std::sort(to_delete.begin(), to_delete.end(), Rowset::comparator);
    if (to_add.size() == to_delete.size()) {
        for (int i = 0; i < to_add.size(); ++i) {
            if (to_add[i]->version() != to_delete[i]->version()) {
                same_version = false;
                break;
            }
        }
    } else {
        same_version = false;
    }

    if (check_delete) {
        for (auto& rs : to_delete) {
            auto find_rs = _rowsets.find(rs->end_version());
            if (find_rs == _rowsets.end()) {
                LOG(WARNING) << "try to delete not exist version " << rs->version() << " from "
                             << full_name();
                return Status::Error<DELETE_VERSION_ERROR>();
            } else if (find_rs->second->rowset_id() != rs->rowset_id()) {
                LOG(WARNING) << "try to delete version " << rs->version() << " from " << full_name()
                             << ", but rowset id changed, delete rowset id is " << rs->rowset_id()
                             << ", exists rowsetid is" << find_rs->second->rowset_id();
                return Status::Error<DELETE_VERSION_ERROR>();
            }
        }
    }

    for (auto& rs : to_delete) {
        _rowsets.erase(rs->end_version());
        if (!same_version) {
            // put compaction rowsets in _stale_rs_version_map.
            _stale_rowsets[rs->version()] = rs;
        }
    }

    for (auto& rs : to_add) {
        _rowsets[rs->end_version()] = rs;
        ++_newly_created_rowset_num;
    }

    // Update rowset tree
    if (keys_type() == UNIQUE_KEYS && enable_unique_key_merge_on_write()) {
        auto new_rowset_tree = std::make_unique<RowsetTree>();
        ModifyRowSetTree(*_rowset_tree, to_delete, to_add, new_rowset_tree.get());
        _rowset_tree = std::move(new_rowset_tree);
    }

    if (!same_version) {
        add_stale_path_version(to_delete);
    } else {
        // delete rowset in "to_delete" directly
        for (auto& rs : to_delete) {
            LOG(INFO) << "add unused rowset " << rs->rowset_id() << " because of same version";
            StorageEngine::instance()->add_unused_rowset(rs);
        }
    }
    return Status::OK();
}

// snapshot manager may call this api to check if version exists, so that
// the version maybe not exist
const RowsetSharedPtr Tablet::get_rowset_by_version(const Version& version,
                                                    bool find_in_stale) const {
    auto iter = _rowsets.find(version.second);
    if (iter == _rowsets.end() || iter->second->start_version() != version.first) {
        if (find_in_stale) {
            return get_stale_rowset_by_version(version);
        }
        return nullptr;
    }
    return iter->second;
}

const RowsetSharedPtr Tablet::get_stale_rowset_by_version(const Version& version) const {
    auto iter = _stale_rowsets.find(version);
    if (iter == _stale_rowsets.end()) {
        VLOG_NOTICE << "no rowset for version:" << version << ", tablet: " << full_name();
        return nullptr;
    }
    return iter->second;
}

// Already under _meta_lock
const RowsetSharedPtr Tablet::rowset_with_max_version() const {
    if (auto it = _rowsets.rbegin(); it != _rowsets.rend()) {
        return it->second;
    }
    return nullptr;
}

RowsetSharedPtr Tablet::rowset_with_max_schema_version(
        const std::vector<RowsetSharedPtr>& rowsets) {
    return *std::max_element(
            rowsets.begin(), rowsets.end(), [](const RowsetSharedPtr& a, const RowsetSharedPtr& b) {
                return !a->tablet_schema()
                               ? true
                               : (!b->tablet_schema()
                                          ? false
                                          : a->tablet_schema()->schema_version() <
                                                    b->tablet_schema()->schema_version());
            });
}

// add inc rowset should not persist tablet meta, because it will be persisted when publish txn.
Status Tablet::add_inc_rowset(const RowsetSharedPtr& rowset) {
    DCHECK(rowset != nullptr);
    std::lock_guard<std::shared_mutex> wrlock(_meta_lock);
    if (contains_version(rowset->version())) {
        return Status::Error<PUSH_VERSION_ALREADY_EXIST>();
    }

    _rowsets[rowset->end_version()] = rowset;

    // Update rowset tree
    if (keys_type() == UNIQUE_KEYS && enable_unique_key_merge_on_write()) {
        auto new_rowset_tree = std::make_unique<RowsetTree>();
        ModifyRowSetTree(*_rowset_tree, {}, {rowset}, new_rowset_tree.get());
        _rowset_tree = std::move(new_rowset_tree);
    }
    ++_newly_created_rowset_num;
    return Status::OK();
}

void Tablet::delete_expired_stale_rowset() {
    using namespace std::chrono;
    int64_t threshold = duration_cast<seconds>(system_clock::now().time_since_epoch()).count() -
                        config::tablet_rowset_stale_sweep_time_sec;
    std::lock_guard<std::shared_mutex> wrlock(_meta_lock);
    auto expired_begin = std::remove_if(_stale_paths.begin(), _stale_paths.end(),
                                        [threshold](auto& path) { return path.ctime < threshold; });
    for (auto it = expired_begin; it != _stale_paths.end(); ++it) {
        for (auto& v : it->versions) {
            if (auto rs_it = _stale_rowsets.find(v); rs_it != _stale_rowsets.end()) {
                _stale_rowsets.erase(rs_it);
            }
        }
    }
    _stale_paths.erase(expired_begin, _stale_paths.end());
#ifndef BE_TEST
    save_meta();
#endif
}

Status Tablet::check_version_integrity(const Version& version, bool quiet) {
    std::shared_lock rdlock(_meta_lock);
    std::vector<RowsetSharedPtr> rowsets;
    return capture_consistent_rowsets(version, &rowsets);
}

bool Tablet::exceed_version_limit(int32_t limit) const {
    if (version_count() > limit) {
        exceed_version_limit_counter << 1;
        return true;
    }
    return false;
}

bool Tablet::contains_version(const Version& version) const {
    auto it = _rowsets.lower_bound(version.second);
    return it != _rowsets.end() && it->second->contains_version(version);
}

// The meta read lock should be held before calling
// FIXME(plat1ko): deprecate?
void Tablet::acquire_version_and_rowsets(
        std::vector<std::pair<Version, RowsetSharedPtr>>* version_rowsets) const {
    for (auto& [v, rs] : _rowsets) {
        version_rowsets->emplace_back(rs->version(), rs);
    }
}

Status Tablet::capture_consistent_rowsets(const Version& spec_version,
                                          std::vector<RowsetSharedPtr>* rowsets) const {
    rowsets->clear();
    // fast path, complex = O(N), N is the size of _rowsets.
    if (auto it = _rowsets.find(spec_version.second);
        it != _rowsets.end() && spec_version.first == 0) {
        for (auto& [v, rs] : _rowsets) {
            if (!rowsets->empty() && rowsets->back()->end_version() + 1 != rs->start_version()) {
                return Status::InternalError("version missed, tablet_id={}", tablet_id());
            }
            rowsets->push_back(rs);
        }
        return Status::OK();
    }
    // complex = O(M*log(N)), N is the size of _stale_rowsets, M is version path length.
    int64_t next_v = spec_version.second;
    do {
        if (auto it = _rowsets.find(next_v); it != _rowsets.end()) {
            auto& rs = it->second;
            if (rs->start_version() > spec_version.first) {
                rowsets->push_back(rs);
                next_v = rs->start_version() - 1;
                continue;
            } else if (rs->start_version() == spec_version.first) {
                rowsets->push_back(rs);
                break;
            }
            // if rs->start_version() < spec_version.first, rowset which start_version = spec_version.first has been merged
        }
        auto it = _stale_rowsets.lower_bound({spec_version.first, next_v});
        if (it == _stale_rowsets.end() || it->first.second != next_v) {
            return Status::InternalError(
                    "could not find a version path, tablet_id={}, spec_version={}", tablet_id(),
                    spec_version.to_string());
        }
        if (it->first.first == spec_version.first) {
            rowsets->push_back(it->second);
            break;
        }
        // rs->start_version > spec_version.first
        rowsets->push_back(it->second);
        next_v = it->first.first - 1;
    } while (true);
    std::reverse(rowsets->begin(), rowsets->end());
    return Status::OK();
}

Status Tablet::capture_rs_readers(const Version& spec_version,
                                  std::vector<RowsetReaderSharedPtr>* rs_readers) const {
    rs_readers->clear();
    std::vector<RowsetSharedPtr> rowsets;
    RETURN_IF_ERROR(capture_consistent_rowsets(spec_version, &rowsets));
    for (auto& rs : rowsets) {
        RowsetReaderSharedPtr reader;
        RETURN_IF_ERROR(rs->create_reader(&reader));
        rs_readers->push_back(std::move(reader));
    }
    return Status::OK();
}

bool Tablet::can_do_compaction(size_t path_hash, CompactionType compaction_type) {
    if (compaction_type == CompactionType::BASE_COMPACTION && tablet_state() != TABLET_RUNNING) {
        // base compaction can only be done for tablet in TABLET_RUNNING state.
        // but cumulative compaction can be done for TABLET_NOTREADY, such as tablet under alter process.
        return false;
    }

    // unique key table with merge-on-write also cann't do cumulative compaction under alter
    // process. It may cause the delete bitmap calculation error, such as two
    // rowsets have same key.
    if (tablet_state() != TABLET_RUNNING && keys_type() == UNIQUE_KEYS &&
        enable_unique_key_merge_on_write()) {
        return false;
    }

    if (data_dir()->path_hash() != path_hash || !is_used()) {
        return false;
    }

    if (tablet_state() == TABLET_NOTREADY) {
        // Before doing schema change, tablet's rowsets that versions smaller than max converting version will be
        // removed. So, we only need to do the compaction when it is being converted.
        // After being converted, tablet's state will be changed to TABLET_RUNNING.
        return SchemaChangeHandler::tablet_in_converting(tablet_id());
    }

    return true;
}

uint32_t Tablet::calc_compaction_score(
        CompactionType compaction_type,
        std::shared_ptr<CumulativeCompactionPolicy> cumulative_compaction_policy) {
    // Need meta lock, because it will iterator "all_rs_metas" of tablet meta.
    std::shared_lock rdlock(_meta_lock);
    if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) {
        return _calc_cumulative_compaction_score(cumulative_compaction_policy);
    } else {
        DCHECK_EQ(compaction_type, CompactionType::BASE_COMPACTION);
        return _calc_base_compaction_score();
    }
}

const uint32_t Tablet::_calc_cumulative_compaction_score(
        std::shared_ptr<CumulativeCompactionPolicy> cumulative_compaction_policy) {
#ifndef BE_TEST
    if (_cumulative_compaction_policy == nullptr ||
        _cumulative_compaction_policy->name() != cumulative_compaction_policy->name()) {
        _cumulative_compaction_policy = cumulative_compaction_policy;
    }
#endif
    uint32_t score = 0;
    _cumulative_compaction_policy->calc_cumulative_compaction_score(
            this, tablet_state(), _tablet_meta->all_rs_metas(), cumulative_layer_point(), &score);
    return score;
}

const uint32_t Tablet::_calc_base_compaction_score() const {
    uint32_t score = 0;
    const int64_t point = cumulative_layer_point();
    auto it = _rowsets.begin();
    // when first rowset start_version != 0, this tablet may be a new tablet in schema change,
    // should not do base compaction.
    if (it == _rowsets.end() || it->second->start_version() != 0) {
        return 0;
    }
    for (; it != _rowsets.end(); ++it) {
        if (it->second->start_version() < point) {
            score += it->second->rowset_meta()->get_compaction_score();
        }
    }
    return score;
}

void Tablet::calc_missed_versions(int64_t spec_version, std::vector<Version>* missed_versions) {
    std::shared_lock rdlock(_meta_lock);
    calc_missed_versions_unlocked(spec_version, missed_versions);
}

// for example:
//     [0-4][5-5][8-8][9-9]
// if spec_version = 6, we still return {7} other than {6, 7}
// TODO(cyx): need to optimize
void Tablet::calc_missed_versions_unlocked(int64_t spec_version,
                                           std::vector<Version>* missed_versions) const {
    DCHECK(spec_version > 0) << "invalid spec_version: " << spec_version;
    std::list<Version> existing_versions;
    for (auto& rs : _tablet_meta->all_rs_metas()) {
        existing_versions.emplace_back(rs->version());
    }

    // sort the existing versions in ascending order
    existing_versions.sort([](const Version& a, const Version& b) {
        // simple because 2 versions are certainly not overlapping
        return a.first < b.first;
    });

    // From the first version(=0),  find the missing version until spec_version
    int64_t last_version = -1;
    for (const Version& version : existing_versions) {
        if (version.first > last_version + 1) {
            for (int64_t i = last_version + 1; i < version.first && i <= spec_version; ++i) {
                missed_versions->emplace_back(Version(i, i));
            }
        }
        last_version = version.second;
        if (last_version >= spec_version) {
            break;
        }
    }
    for (int64_t i = last_version + 1; i <= spec_version; ++i) {
        missed_versions->emplace_back(Version(i, i));
    }
}

void Tablet::max_continuous_version_from_beginning(Version* version) {
    bool has_version_cross;
    std::shared_lock rdlock(_meta_lock);
    _max_continuous_version_from_beginning_unlocked(version, &has_version_cross);
}

void Tablet::_max_continuous_version_from_beginning_unlocked(Version* version,
                                                             bool* has_version_cross) const {
    *version = {-1, -1};
    for (auto& [v, rs] : _rowsets) {
        if (rs->start_version() > version->second + 1) {
            break;
        } else if (rs->start_version() <= version->second) {
            *has_version_cross = true;
        }
        *version = rs->version();
    }
}

void Tablet::calculate_cumulative_point() {
    std::lock_guard<std::shared_mutex> wrlock(_meta_lock);
    int64_t ret_cumulative_point;
    _cumulative_compaction_policy->calculate_cumulative_point(
            this, _tablet_meta->all_rs_metas(), _cumulative_point, &ret_cumulative_point);

    if (ret_cumulative_point == K_INVALID_CUMULATIVE_POINT) {
        return;
    }
    set_cumulative_layer_point(ret_cumulative_point);
}

// NOTE: only used when create_table, so it is sure that there is no concurrent reader and writer.
void Tablet::delete_all_files() {
    // Release resources like memory and disk space.
    std::shared_lock rdlock(_meta_lock);
    for (auto& [v, rs] : _rowsets) {
        rs->remove();
    }
    _rowsets.clear();

    for (auto& [v, rs] : _stale_rowsets) {
        rs->remove();
    }
    _stale_rowsets.clear();

    if (keys_type() == UNIQUE_KEYS && enable_unique_key_merge_on_write()) {
        // clear rowset_tree
        _rowset_tree = std::make_unique<RowsetTree>();
    }
}

bool Tablet::check_path(const std::string& path_to_check) const {
    std::shared_lock rdlock(_meta_lock);
    if (path_to_check == _tablet_path) {
        return true;
    }
    auto tablet_id_dir = io::Path(_tablet_path).parent_path();
    if (path_to_check == tablet_id_dir) {
        return true;
    }
    for (auto& [v, rs] : _rowsets) {
        bool ret = rs->check_path(path_to_check);
        if (ret) {
            return true;
        }
    }
    for (auto& [v, rs] : _stale_rowsets) {
        bool ret = rs->check_path(path_to_check);
        if (ret) {
            return true;
        }
    }
    return false;
}

// check rowset id in tablet-meta and in rowset-meta atomicly
// for example, during publish version stage, it will first add rowset meta to tablet meta and then
// remove it from rowset meta manager. If we check tablet meta first and then check rowset meta using 2 step unlocked
// the sequence maybe: 1. check in tablet meta [return false]  2. add to tablet meta  3. remove from rowset meta manager
// 4. check in rowset meta manager return false. so that the rowset maybe checked return false it means it is useless and
// will be treated as a garbage.
bool Tablet::check_rowset_id(const RowsetId& rowset_id) {
    std::shared_lock rdlock(_meta_lock);
    if (StorageEngine::instance()->rowset_id_in_use(rowset_id)) {
        return true;
    }
    for (auto& [v, rs] : _rowsets) {
        if (rs->rowset_id() == rowset_id) {
            return true;
        }
    }
    for (auto& [v, rs] : _stale_rowsets) {
        if (rs->rowset_id() == rowset_id) {
            return true;
        }
    }
    if (RowsetMetaManager::check_rowset_meta(_data_dir->get_meta(), tablet_uid(), rowset_id)) {
        return true;
    }
    return false;
}

void Tablet::_print_missed_versions(const std::vector<Version>& missed_versions) const {
    std::stringstream ss;
    ss << full_name() << " has " << missed_versions.size() << " missed version:";
    // print at most 10 version
    for (int i = 0; i < 10 && i < missed_versions.size(); ++i) {
        ss << missed_versions[i] << ",";
    }
    LOG(WARNING) << ss.str();
}

Status Tablet::set_partition_id(int64_t partition_id) {
    return _tablet_meta->set_partition_id(partition_id);
}

TabletInfo Tablet::get_tablet_info() const {
    return TabletInfo(tablet_id(), schema_hash(), tablet_uid());
}

std::vector<RowsetSharedPtr> Tablet::pick_candidate_rowsets_to_cumulative_compaction() {
    std::vector<RowsetSharedPtr> candidate_rowsets;
    std::shared_lock rlock(_meta_lock);
    int64_t cumulative_point = _cumulative_point.load(std::memory_order_relaxed);
    if (cumulative_point == K_INVALID_CUMULATIVE_POINT) {
        return candidate_rowsets;
    }
    auto it = _rowsets.lower_bound(cumulative_point);
    if (it != _rowsets.end() && it->second->start_version() < cumulative_point) ++it;
    for (; it != _rowsets.end(); ++it) {
        if (it->second->is_local()) {
            candidate_rowsets.push_back(it->second);
        }
    }
    return candidate_rowsets;
}

std::vector<RowsetSharedPtr> Tablet::pick_candidate_rowsets_to_base_compaction() {
    std::vector<RowsetSharedPtr> candidate_rowsets;
    std::shared_lock rlock(_meta_lock);
    int64_t cumulative_point = _cumulative_point.load(std::memory_order_relaxed);
    for (auto& [v, rs] : _rowsets) {
        // Do compaction on local rowsets only.
        if (v < cumulative_point && rs->is_local()) {
            candidate_rowsets.push_back(rs);
        }
    }
    return candidate_rowsets;
}

// For http compaction action
void Tablet::get_compaction_status(std::string* json_result) {
    rapidjson::Document root;
    root.SetObject();

    rapidjson::Document path_arr;
    path_arr.SetArray();

    std::vector<RowsetSharedPtr> rowsets;
    std::vector<RowsetSharedPtr> stale_rowsets;
    std::vector<bool> delete_flags;
    {
        std::shared_lock rdlock(_meta_lock);
        rowsets.reserve(_rowsets.size());
        for (auto& [v, rs] : _rowsets) {
            rowsets.push_back(rs);
        }
        stale_rowsets.reserve(_stale_rowsets.size());
        for (auto& [v, rs] : _rowsets) {
            stale_rowsets.push_back(rs);
        }
        delete_flags.reserve(rowsets.size());
        for (auto& rs : rowsets) {
            delete_flags.push_back(rs->rowset_meta()->has_delete_predicate());
        }
    }
    rapidjson::Value cumulative_policy_type;
    std::string policy_type_str = "cumulative compaction policy not initializied";
    if (_cumulative_compaction_policy != nullptr) {
        policy_type_str = _cumulative_compaction_policy->name();
    }
    cumulative_policy_type.SetString(policy_type_str.c_str(), policy_type_str.length(),
                                     root.GetAllocator());
    root.AddMember("cumulative policy type", cumulative_policy_type, root.GetAllocator());
    root.AddMember("cumulative point", _cumulative_point.load(), root.GetAllocator());
    rapidjson::Value cumu_value;
    std::string format_str = ToStringFromUnixMillis(_last_cumu_compaction_failure_millis.load());
    cumu_value.SetString(format_str.c_str(), format_str.length(), root.GetAllocator());
    root.AddMember("last cumulative failure time", cumu_value, root.GetAllocator());
    rapidjson::Value base_value;
    format_str = ToStringFromUnixMillis(_last_base_compaction_failure_millis.load());
    base_value.SetString(format_str.c_str(), format_str.length(), root.GetAllocator());
    root.AddMember("last base failure time", base_value, root.GetAllocator());
    rapidjson::Value cumu_success_value;
    format_str = ToStringFromUnixMillis(_last_cumu_compaction_success_millis.load());
    cumu_success_value.SetString(format_str.c_str(), format_str.length(), root.GetAllocator());
    root.AddMember("last cumulative success time", cumu_success_value, root.GetAllocator());
    rapidjson::Value base_success_value;
    format_str = ToStringFromUnixMillis(_last_base_compaction_success_millis.load());
    base_success_value.SetString(format_str.c_str(), format_str.length(), root.GetAllocator());
    root.AddMember("last base success time", base_success_value, root.GetAllocator());

    // print all rowsets' version as an array
    rapidjson::Document versions_arr;
    rapidjson::Document missing_versions_arr;
    versions_arr.SetArray();
    missing_versions_arr.SetArray();
    int64_t last_version = -1;
    for (int i = 0; i < rowsets.size(); ++i) {
        const Version& ver = rowsets[i]->version();
        if (ver.first != last_version + 1) {
            rapidjson::Value miss_value;
            miss_value.SetString(
                    strings::Substitute("[$0-$1]", last_version + 1, ver.first - 1).c_str(),
                    missing_versions_arr.GetAllocator());
            missing_versions_arr.PushBack(miss_value, missing_versions_arr.GetAllocator());
        }
        rapidjson::Value value;
        std::string disk_size = PrettyPrinter::print(
                static_cast<uint64_t>(rowsets[i]->rowset_meta()->total_disk_size()), TUnit::BYTES);
        std::string version_str = strings::Substitute(
                "[$0-$1] $2 $3 $4 $5 $6", ver.first, ver.second, rowsets[i]->num_segments(),
                (delete_flags[i] ? "DELETE" : "DATA"),
                SegmentsOverlapPB_Name(rowsets[i]->rowset_meta()->segments_overlap()),
                rowsets[i]->rowset_id().to_string(), disk_size);
        value.SetString(version_str.c_str(), version_str.length(), versions_arr.GetAllocator());
        versions_arr.PushBack(value, versions_arr.GetAllocator());
        last_version = ver.second;
    }
    root.AddMember("rowsets", versions_arr, root.GetAllocator());
    root.AddMember("missing_rowsets", missing_versions_arr, root.GetAllocator());

    // print all stale rowsets' version as an array
    rapidjson::Document stale_versions_arr;
    stale_versions_arr.SetArray();
    for (int i = 0; i < stale_rowsets.size(); ++i) {
        const Version& ver = stale_rowsets[i]->version();
        rapidjson::Value value;
        std::string disk_size = PrettyPrinter::print(
                static_cast<uint64_t>(stale_rowsets[i]->rowset_meta()->total_disk_size()),
                TUnit::BYTES);
        std::string version_str = strings::Substitute(
                "[$0-$1] $2 $3 $4", ver.first, ver.second, stale_rowsets[i]->num_segments(),
                stale_rowsets[i]->rowset_id().to_string(), disk_size);
        value.SetString(version_str.c_str(), version_str.length(),
                        stale_versions_arr.GetAllocator());
        stale_versions_arr.PushBack(value, stale_versions_arr.GetAllocator());
    }
    root.AddMember("stale_rowsets", stale_versions_arr, root.GetAllocator());

    // add stale version rowsets
    root.AddMember("stale version path", path_arr, root.GetAllocator());

    // to json string
    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    *json_result = std::string(strbuf.GetString());
}

bool Tablet::do_tablet_meta_checkpoint() {
    std::lock_guard<std::shared_mutex> store_lock(_meta_store_lock);
    if (_newly_created_rowset_num == 0) {
        return false;
    }
    if (UnixMillis() - _last_checkpoint_time <
                config::tablet_meta_checkpoint_min_interval_secs * 1000 &&
        _newly_created_rowset_num < config::tablet_meta_checkpoint_min_new_rowsets_num) {
        return false;
    }

    // hold read-lock other than write-lock, because it will not modify meta structure
    std::shared_lock rdlock(_meta_lock);
    if (tablet_state() != TABLET_RUNNING) {
        LOG(INFO) << "tablet is under state=" << tablet_state()
                  << ", not running, skip do checkpoint"
                  << ", tablet=" << full_name();
        return false;
    }
    VLOG_NOTICE << "start to do tablet meta checkpoint, tablet=" << full_name();
    save_meta();
    // if save meta successfully, then should remove the rowset meta existing in tablet
    // meta from rowset meta store
    for (auto& [v, rs] : _rowsets) {
        auto& rs_meta = rs->rowset_meta();
        // If we delete it from rowset manager's meta explicitly in previous checkpoint, just skip.
        if (rs_meta->is_remove_from_rowset_meta()) {
            continue;
        }
        if (RowsetMetaManager::check_rowset_meta(_data_dir->get_meta(), tablet_uid(),
                                                 rs_meta->rowset_id())) {
            RowsetMetaManager::remove(_data_dir->get_meta(), tablet_uid(), rs_meta->rowset_id());
            VLOG_NOTICE << "remove rowset id from meta store because it is already persistent with "
                        << "tablet meta, rowset_id=" << rs_meta->rowset_id();
        }
        rs_meta->set_remove_from_rowset_meta();
    }

    // check _stale_rs_version_map to remove meta from rowset meta store
    for (auto& [v, rs] : _stale_rowsets) {
        auto& rs_meta = rs->rowset_meta();
        // If we delete it from rowset manager's meta explicitly in previous checkpoint, just skip.
        if (rs_meta->is_remove_from_rowset_meta()) {
            continue;
        }
        if (RowsetMetaManager::check_rowset_meta(_data_dir->get_meta(), tablet_uid(),
                                                 rs_meta->rowset_id())) {
            RowsetMetaManager::remove(_data_dir->get_meta(), tablet_uid(), rs_meta->rowset_id());
            VLOG_NOTICE << "remove rowset id from meta store because it is already persistent with "
                        << "tablet meta, rowset_id=" << rs_meta->rowset_id();
        }
        rs_meta->set_remove_from_rowset_meta();
    }

    _newly_created_rowset_num = 0;
    _last_checkpoint_time = UnixMillis();
    return true;
}

bool Tablet::rowset_meta_is_useful(RowsetMetaSharedPtr rowset_meta) {
    std::shared_lock rdlock(_meta_lock);
    bool find_version = false;
    for (auto& [v, rs] : _rowsets) {
        if (rs->rowset_id() == rowset_meta->rowset_id()) {
            return true;
        }
        if (rs->contains_version(rowset_meta->version())) {
            find_version = true;
        }
    }
    for (auto& [v, rs] : _stale_rowsets) {
        if (rs->rowset_id() == rowset_meta->rowset_id()) {
            return true;
        }
        if (rs->contains_version(rowset_meta->version())) {
            find_version = true;
        }
    }
    return !find_version;
}

// need check if consecutive version missing in full report
// alter tablet will ignore this check
void Tablet::build_tablet_report_info(TTabletInfo* tablet_info,
                                      bool enable_consecutive_missing_check) {
    std::shared_lock rdlock(_meta_lock);
    tablet_info->tablet_id = _tablet_meta->tablet_id();
    tablet_info->schema_hash = _tablet_meta->schema_hash();
    tablet_info->row_count = num_rows();
    tablet_info->data_size = tablet_local_size();

    // Here we need to report to FE if there are any missing versions of tablet.
    // We start from the initial version and traverse backwards until we meet a discontinuous version.
    Version cversion;
    Version max_version = max_version_unlocked();
    bool has_version_cross;
    _max_continuous_version_from_beginning_unlocked(&cversion, &has_version_cross);
    // cause publish version task runs concurrently, version may be flying
    // so we add a consecutive miss check to solve this problem:
    // if publish version 5 arrives but version 4 flying, we may judge replica miss version
    // and set version miss in tablet_info, which makes fe treat this replica as unhealth
    // and lead to other problems
    if (enable_consecutive_missing_check) {
        if (cversion.second < max_version.second) {
            if (_last_missed_version == cversion.second + 1) {
                if (MonotonicSeconds() - _last_missed_time_s >= 60) {
                    // version missed for over 60 seconds
                    tablet_info->__set_version_miss(true);
                    _last_missed_version = -1;
                    _last_missed_time_s = 0;
                }
            } else {
                _last_missed_version = cversion.second + 1;
                _last_missed_time_s = MonotonicSeconds();
            }
        }
    } else {
        tablet_info->__set_version_miss(cversion.second < max_version.second);
    }

    if (has_version_cross && tablet_state() == TABLET_RUNNING) {
        tablet_info->__set_used(false);
    }

    if (tablet_state() == TABLET_SHUTDOWN) {
        tablet_info->__set_used(false);
    }

    // the report version is the largest continuous version, same logic as in FE side
    tablet_info->version = cversion.second;
    // Useless but it is a required filed in TTabletInfo
    tablet_info->version_hash = 0;
    tablet_info->__set_partition_id(_tablet_meta->partition_id());
    tablet_info->__set_storage_medium(_data_dir->storage_medium());
    tablet_info->__set_version_count(version_count());
    tablet_info->__set_path_hash(_data_dir->path_hash());
    tablet_info->__set_is_in_memory(_tablet_meta->tablet_schema()->is_in_memory());
    tablet_info->__set_replica_id(replica_id());
    tablet_info->__set_remote_data_size(tablet_remote_size());
}

// should use this method to get a copy of current tablet meta
// there are some rowset meta in local meta store and in in-memory tablet meta
// but not in tablet meta in local meta store
void Tablet::generate_tablet_meta_copy(TabletMetaSharedPtr new_tablet_meta) const {
    std::shared_lock rdlock(_meta_lock);
    generate_tablet_meta_copy_unlocked(new_tablet_meta);
}

// this is a unlocked version of generate_tablet_meta_copy()
// some method already hold the _meta_lock before calling this,
// such as EngineCloneTask::_finish_clone -> tablet->revise_tablet_meta
void Tablet::generate_tablet_meta_copy_unlocked(TabletMetaSharedPtr new_tablet_meta) const {
    TabletMetaPB tablet_meta_pb;
    _tablet_meta->to_meta_pb(&tablet_meta_pb);
    new_tablet_meta->init_from_pb(tablet_meta_pb);
}

Status Tablet::prepare_compaction_and_calculate_permits(CompactionType compaction_type,
                                                        TabletSharedPtr tablet, int64_t* permits) {
    std::vector<RowsetSharedPtr> compaction_rowsets;
    if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) {
        scoped_refptr<Trace> trace(new Trace);
        MonotonicStopWatch watch;
        watch.start();
        SCOPED_CLEANUP({
            if (watch.elapsed_time() / 1e9 > config::cumulative_compaction_trace_threshold) {
                LOG(WARNING) << "Trace:" << std::endl << trace->DumpToString(Trace::INCLUDE_ALL);
            }
        });
        ADOPT_TRACE(trace.get());

        TRACE("create cumulative compaction");
        StorageEngine::instance()->create_cumulative_compaction(tablet, _cumulative_compaction);
        DorisMetrics::instance()->cumulative_compaction_request_total->increment(1);
        Status res = _cumulative_compaction->prepare_compact();
        if (!res.ok()) {
            set_last_cumu_compaction_failure_time(UnixMillis());
            *permits = 0;
            if (!res.is<CUMULATIVE_NO_SUITABLE_VERSION>()) {
                DorisMetrics::instance()->cumulative_compaction_request_failed->increment(1);
                return Status::InternalError("prepare cumulative compaction with err: {}", res);
            }
            // return OK if OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSION, so that we don't need to
            // print too much useless logs.
            // And because we set permits to 0, so even if we return OK here, nothing will be done.
            return Status::OK();
        }
        compaction_rowsets = _cumulative_compaction->get_input_rowsets();
    } else {
        DCHECK_EQ(compaction_type, CompactionType::BASE_COMPACTION);
        scoped_refptr<Trace> trace(new Trace);
        MonotonicStopWatch watch;
        watch.start();
        SCOPED_CLEANUP({
            if (watch.elapsed_time() / 1e9 > config::base_compaction_trace_threshold) {
                LOG(WARNING) << "Trace:" << std::endl << trace->DumpToString(Trace::INCLUDE_ALL);
            }
        });
        ADOPT_TRACE(trace.get());

        TRACE("create base compaction");
        StorageEngine::instance()->create_base_compaction(tablet, _base_compaction);
        DorisMetrics::instance()->base_compaction_request_total->increment(1);
        Status res = _base_compaction->prepare_compact();
        if (!res.ok()) {
            set_last_base_compaction_failure_time(UnixMillis());
            *permits = 0;
            if (!res.is<BE_NO_SUITABLE_VERSION>()) {
                DorisMetrics::instance()->base_compaction_request_failed->increment(1);
                return Status::InternalError("prepare base compaction with err: {}", res);
            }
            // return OK if OLAP_ERR_BE_NO_SUITABLE_VERSION, so that we don't need to
            // print too much useless logs.
            // And because we set permits to 0, so even if we return OK here, nothing will be done.
            return Status::OK();
        }
        compaction_rowsets = _base_compaction->get_input_rowsets();
    }
    *permits = 0;
    for (auto rowset : compaction_rowsets) {
        *permits += rowset->rowset_meta()->get_compaction_score();
    }
    return Status::OK();
}

void Tablet::execute_compaction(CompactionType compaction_type) {
    if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) {
        scoped_refptr<Trace> trace(new Trace);
        MonotonicStopWatch watch;
        watch.start();
        SCOPED_CLEANUP({
            if (!config::disable_compaction_trace_log &&
                watch.elapsed_time() / 1e9 > config::cumulative_compaction_trace_threshold) {
                LOG(WARNING) << "Trace:" << std::endl << trace->DumpToString(Trace::INCLUDE_ALL);
            }
        });
        ADOPT_TRACE(trace.get());

        TRACE("execute cumulative compaction");
        Status res = _cumulative_compaction->execute_compact();
        if (!res.ok()) {
            set_last_cumu_compaction_failure_time(UnixMillis());
            DorisMetrics::instance()->cumulative_compaction_request_failed->increment(1);
            LOG(WARNING) << "failed to do cumulative compaction. res=" << res
                         << ", tablet=" << full_name();
            return;
        }
        set_last_cumu_compaction_failure_time(0);
    } else {
        DCHECK_EQ(compaction_type, CompactionType::BASE_COMPACTION);
        scoped_refptr<Trace> trace(new Trace);
        MonotonicStopWatch watch;
        watch.start();
        SCOPED_CLEANUP({
            if (!config::disable_compaction_trace_log &&
                watch.elapsed_time() / 1e9 > config::base_compaction_trace_threshold) {
                LOG(WARNING) << "Trace:" << std::endl << trace->DumpToString(Trace::INCLUDE_ALL);
            }
        });
        ADOPT_TRACE(trace.get());

        TRACE("create base compaction");
        Status res = _base_compaction->execute_compact();
        if (!res.ok()) {
            set_last_base_compaction_failure_time(UnixMillis());
            DorisMetrics::instance()->base_compaction_request_failed->increment(1);
            LOG(WARNING) << "failed to do base compaction. res=" << res
                         << ", tablet=" << full_name();
            return;
        }
        set_last_base_compaction_failure_time(0);
    }
}

void Tablet::reset_compaction(CompactionType compaction_type) {
    if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) {
        _cumulative_compaction.reset();
    } else {
        _base_compaction.reset();
    }
}

Status Tablet::create_initial_rowset(const int64_t req_version) {
    Status res = Status::OK();
    if (req_version < 1) {
        LOG(WARNING) << "init version of tablet should at least 1. req.ver=" << req_version;
        return Status::Error<CE_CMD_PARAMS_ERROR>();
    }
    Version version(0, req_version);
    RowsetSharedPtr new_rowset;
    do {
        // there is no data in init rowset, so overlapping info is unknown.
        std::unique_ptr<RowsetWriter> rs_writer;
        RowsetWriterContext context;
        context.version = version;
        context.rowset_state = VISIBLE;
        context.segments_overlap = OVERLAP_UNKNOWN;
        context.tablet_schema = tablet_schema();
        res = create_rowset_writer(context, &rs_writer);

        if (!res.ok()) {
            LOG(WARNING) << "failed to init rowset writer for tablet " << full_name();
            break;
        }
        res = rs_writer->flush();
        if (!res.ok()) {
            LOG(WARNING) << "failed to flush rowset writer for tablet " << full_name();
            break;
        }

        new_rowset = rs_writer->build();
        res = add_rowset(new_rowset);
        if (!res.ok()) {
            LOG(WARNING) << "failed to add rowset for tablet " << full_name();
            break;
        }
    } while (0);

    // Unregister index and delete files(index and data) if failed
    if (!res.ok()) {
        LOG(WARNING) << "fail to create initial rowset. res=" << res << " version=" << req_version;
        StorageEngine::instance()->add_unused_rowset(new_rowset);
        return res;
    }
    set_cumulative_layer_point(req_version + 1);
    return res;
}

Status Tablet::create_vertical_rowset_writer(RowsetWriterContext& context,
                                             std::unique_ptr<RowsetWriter>* rowset_writer) {
    _init_context_common_fields(context);
    return RowsetFactory::create_rowset_writer(context, true, rowset_writer);
}

Status Tablet::create_rowset_writer(RowsetWriterContext& context,
                                    std::unique_ptr<RowsetWriter>* rowset_writer) {
    _init_context_common_fields(context);
    return RowsetFactory::create_rowset_writer(context, false, rowset_writer);
}

void Tablet::_init_context_common_fields(RowsetWriterContext& context) {
    context.rowset_id = StorageEngine::instance()->next_rowset_id();
    context.tablet_uid = tablet_uid();

    context.tablet_id = tablet_id();
    context.partition_id = partition_id();
    context.tablet_schema_hash = schema_hash();
    context.rowset_type = tablet_meta()->preferred_rowset_type();
    // Alpha Rowset will be removed in the future, so that if the tablet's default rowset type is
    // alpah rowset, then set the newly created rowset to storage engine's default rowset.
    if (context.rowset_type == ALPHA_ROWSET) {
        context.rowset_type = StorageEngine::instance()->default_rowset_type();
    }
    if (context.fs != nullptr && context.fs->type() != io::FileSystemType::LOCAL) {
        context.rowset_dir = BetaRowset::remote_tablet_path(tablet_id());
    } else {
        context.rowset_dir = tablet_path();
    }
    context.data_dir = data_dir();
    context.enable_unique_key_merge_on_write = enable_unique_key_merge_on_write();
}

Status Tablet::create_rowset(RowsetMetaSharedPtr rowset_meta, RowsetSharedPtr* rowset) {
    return RowsetFactory::create_rowset(tablet_schema(), tablet_path(), rowset_meta, rowset);
}

Status Tablet::cooldown() {
    std::unique_lock schema_change_lock(_schema_change_lock, std::try_to_lock);
    if (!schema_change_lock.owns_lock()) {
        LOG(WARNING) << "Failed to own schema_change_lock. tablet=" << tablet_id();
        return Status::Error<TRY_LOCK_FAILED>();
    }
    // Check executing serially with compaction task.
    std::unique_lock base_compaction_lock(_base_compaction_lock, std::try_to_lock);
    if (!base_compaction_lock.owns_lock()) {
        LOG(WARNING) << "Failed to own base_compaction_lock. tablet=" << tablet_id();
        return Status::Error<TRY_LOCK_FAILED>();
    }
    std::unique_lock cumu_compaction_lock(_cumulative_compaction_lock, std::try_to_lock);
    if (!cumu_compaction_lock.owns_lock()) {
        LOG(WARNING) << "Failed to own cumu_compaction_lock. tablet=" << tablet_id();
        return Status::Error<TRY_LOCK_FAILED>();
    }
    auto dest_fs = io::FileSystemMap::instance()->get(storage_policy());
    if (!dest_fs) {
        return Status::Error<UNINITIALIZED>();
    }
    DCHECK(dest_fs->type() == io::FileSystemType::S3);
    auto old_rowset = pick_cooldown_rowset();
    if (!old_rowset) {
        LOG(WARNING) << "Cannot pick cooldown rowset in tablet " << tablet_id();
        return Status::OK();
    }
    RowsetId new_rowset_id = StorageEngine::instance()->next_rowset_id();

    auto start = std::chrono::steady_clock::now();

    auto st = old_rowset->upload_to(reinterpret_cast<io::RemoteFileSystem*>(dest_fs.get()),
                                    new_rowset_id);
    if (!st.ok()) {
        record_unused_remote_rowset(new_rowset_id, dest_fs->resource_id(),
                                    old_rowset->num_segments());
        return st;
    }

    auto duration = std::chrono::duration<float>(std::chrono::steady_clock::now() - start);
    LOG(INFO) << "Upload rowset " << old_rowset->version() << " " << new_rowset_id.to_string()
              << " to " << dest_fs->root_path().native() << ", tablet_id=" << tablet_id()
              << ", duration=" << duration.count() << ", capacity=" << old_rowset->data_disk_size()
              << ", tp=" << old_rowset->data_disk_size() / duration.count();

    // gen a new rowset
    auto new_rowset_meta = std::make_shared<RowsetMeta>(*old_rowset->rowset_meta());
    new_rowset_meta->set_rowset_id(new_rowset_id);
    new_rowset_meta->set_resource_id(dest_fs->resource_id());
    new_rowset_meta->set_fs(dest_fs);
    new_rowset_meta->set_creation_time(time(nullptr));
    RowsetSharedPtr new_rowset;
    RowsetFactory::create_rowset(_schema, _tablet_path, new_rowset_meta, &new_rowset);

    std::vector to_add {std::move(new_rowset)};
    std::vector to_delete {std::move(old_rowset)};

    bool has_shutdown = false;
    {
        std::unique_lock meta_wlock(_meta_lock);
        has_shutdown = tablet_state() == TABLET_SHUTDOWN;
        if (!has_shutdown) {
            modify_rowsets(to_add, to_delete);
            _self_owned_remote_rowsets.insert(to_add.front());
            save_meta();
        }
    }
    if (has_shutdown) {
        record_unused_remote_rowset(new_rowset_id, dest_fs->resource_id(),
                                    to_add.front()->num_segments());
        return Status::Aborted("tablet {} has shutdown", tablet_id());
    }
    return Status::OK();
}

RowsetSharedPtr Tablet::pick_cooldown_rowset() {
    std::shared_lock meta_rlock(_meta_lock);
    // We pick the rowset with smallest start version in local.
    for (auto& [v, rs] : _rowsets) {
        if (rs->is_local()) {
            return rs;
        }
    }
    return nullptr;
}

bool Tablet::need_cooldown(int64_t* cooldown_timestamp, size_t* file_size) {
    // std::shared_lock meta_rlock(_meta_lock);
    if (storage_policy().empty()) {
        VLOG_DEBUG << "tablet does not need cooldown, tablet id: " << tablet_id();
        return false;
    }
    auto policy = ExecEnv::GetInstance()->storage_policy_mgr()->get(storage_policy());
    if (!policy) {
        LOG(WARNING) << "Cannot get storage policy: " << storage_policy();
        return false;
    }
    auto cooldown_ttl_sec = policy->cooldown_ttl;
    auto cooldown_datetime = policy->cooldown_datetime;
    RowsetSharedPtr rowset = pick_cooldown_rowset();
    if (!rowset) {
        VLOG_DEBUG << "pick cooldown rowset, get null, tablet id: " << tablet_id();
        return false;
    }

    int64_t oldest_cooldown_time = std::numeric_limits<int64_t>::max();
    if (cooldown_ttl_sec >= 0) {
        oldest_cooldown_time = rowset->oldest_write_timestamp() + cooldown_ttl_sec;
    }
    if (cooldown_datetime > 0) {
        oldest_cooldown_time = std::min(oldest_cooldown_time, cooldown_datetime);
    }

    int64_t newest_cooldown_time = std::numeric_limits<int64_t>::max();
    if (cooldown_ttl_sec >= 0) {
        newest_cooldown_time = rowset->newest_write_timestamp() + cooldown_ttl_sec;
    }
    if (cooldown_datetime > 0) {
        newest_cooldown_time = std::min(newest_cooldown_time, cooldown_datetime);
    }

    if (oldest_cooldown_time + config::cooldown_lag_time_sec < UnixSeconds()) {
        *cooldown_timestamp = oldest_cooldown_time;
        VLOG_DEBUG << "tablet need cooldown, tablet id: " << tablet_id()
                   << " cooldown_timestamp: " << *cooldown_timestamp;
        return true;
    }

    if (newest_cooldown_time < UnixSeconds()) {
        *file_size = rowset->data_disk_size();
        VLOG_DEBUG << "tablet need cooldown, tablet id: " << tablet_id()
                   << " file_size: " << *file_size;
        return true;
    }

    VLOG_DEBUG << "tablet does not need cooldown, tablet id: " << tablet_id()
               << " ttl sec: " << cooldown_ttl_sec << " cooldown datetime: " << cooldown_datetime
               << " oldest write time: " << rowset->oldest_write_timestamp()
               << " newest write time: " << rowset->newest_write_timestamp();
    return false;
}

void Tablet::record_unused_remote_rowset(const RowsetId& rowset_id, const io::ResourceId& resource,
                                         int64_t num_segments) {
    auto gc_key = REMOTE_ROWSET_GC_PREFIX + rowset_id.to_string();
    RemoteRowsetGcPB gc_pb;
    gc_pb.set_resource_id(resource);
    gc_pb.set_tablet_id(tablet_id());
    gc_pb.set_num_segments(num_segments);
    WARN_IF_ERROR(
            _data_dir->get_meta()->put(META_COLUMN_FAMILY_INDEX, gc_key, gc_pb.SerializeAsString()),
            fmt::format("Failed to record unused remote rowset(tablet id: {}, rowset id: {})",
                        tablet_id(), rowset_id.to_string()));
}

Status Tablet::remove_all_remote_rowsets() {
    DCHECK(tablet_state() == TABLET_SHUTDOWN);
    if (storage_policy().empty()) {
        return Status::OK();
    }
    auto tablet_gc_key = REMOTE_TABLET_GC_PREFIX + std::to_string(tablet_id());
    return _data_dir->get_meta()->put(META_COLUMN_FAMILY_INDEX, tablet_gc_key, storage_policy());
}

TabletSchemaSPtr Tablet::tablet_schema() const {
    std::shared_lock wrlock(_meta_lock);
    return _max_version_schema;
}

void Tablet::update_max_version_schema(const TabletSchemaSPtr& tablet_schema) {
    std::lock_guard wrlock(_meta_lock);
    // Double Check for concurrent update
    if (!_max_version_schema ||
        tablet_schema->schema_version() > _max_version_schema->schema_version()) {
        _max_version_schema = tablet_schema;
    }
}

TabletSchemaSPtr Tablet::get_max_version_schema(std::lock_guard<std::shared_mutex>&) {
    return _max_version_schema;
}

Status Tablet::lookup_row_key(const Slice& encoded_key, const RowsetIdUnorderedSet* rowset_ids,
                              RowLocation* row_location, uint32_t version) {
    std::vector<std::pair<RowsetSharedPtr, int32_t>> selected_rs;
    size_t seq_col_length = 0;
    if (_schema->has_sequence_col()) {
        seq_col_length = _schema->column(_schema->sequence_col_idx()).length() + 1;
    }
    Slice key_without_seq = Slice(encoded_key.get_data(), encoded_key.get_size() - seq_col_length);
    _rowset_tree->FindRowsetsWithKeyInRange(key_without_seq, rowset_ids, &selected_rs);
    if (selected_rs.empty()) {
        return Status::NotFound("No rowsets contains the key in key range");
    }
    // Usually newly written data has a higher probability of being modified, so prefer
    // to search the key in the rowset with larger version.
    std::sort(selected_rs.begin(), selected_rs.end(),
              [](std::pair<RowsetSharedPtr, int32_t>& a, std::pair<RowsetSharedPtr, int32_t>& b) {
                  if (a.first->end_version() == b.first->end_version()) {
                      return a.second > b.second;
                  }
                  return a.first->end_version() > b.first->end_version();
              });
    RowLocation loc;
    for (auto& rs : selected_rs) {
        if (rs.first->end_version() > version) {
            continue;
        }
        SegmentCacheHandle segment_cache_handle;
        RETURN_NOT_OK(SegmentLoader::instance()->load_segments(
                std::static_pointer_cast<BetaRowset>(rs.first), &segment_cache_handle, true));
        auto& segments = segment_cache_handle.get_segments();
        DCHECK_GT(segments.size(), rs.second);
        Status s = segments[rs.second]->lookup_row_key(encoded_key, &loc);
        if (s.is<NOT_FOUND>()) {
            continue;
        }
        if (!s.ok()) {
            return s;
        }
        loc.rowset_id = rs.first->rowset_id();
        if (version >= 0 && _tablet_meta->delete_bitmap().contains_agg(
                                    {loc.rowset_id, loc.segment_id, version}, loc.row_id)) {
            // if has sequence col, we continue to compare the sequence_id of
            // all rowsets, util we find an existing key.
            if (_schema->has_sequence_col()) {
                continue;
            }
            // The key is deleted, we don't need to search for it any more.
            break;
        }
        *row_location = loc;
        // find it and return
        return s;
    }
    return Status::NotFound("can't find key in all rowsets");
}

// load segment may do io so it should out lock
Status Tablet::_load_rowset_segments(const RowsetSharedPtr& rowset,
                                     std::vector<segment_v2::SegmentSharedPtr>* segments) {
    auto beta_rowset = reinterpret_cast<BetaRowset*>(rowset.get());
    RETURN_IF_ERROR(beta_rowset->load_segments(segments));
    return Status::OK();
}

// caller should hold meta_lock
Status Tablet::calc_delete_bitmap(RowsetId rowset_id,
                                  const std::vector<segment_v2::SegmentSharedPtr>& segments,
                                  const RowsetIdUnorderedSet* specified_rowset_ids,
                                  DeleteBitmapPtr delete_bitmap, int64_t end_version,
                                  bool check_pre_segments) {
    std::vector<segment_v2::SegmentSharedPtr> pre_segments;
    OlapStopWatch watch;

    Version dummy_version(end_version + 1, end_version + 1);
    for (auto& seg : segments) {
        seg->load_pk_index_and_bf(); // We need index blocks to iterate
        auto pk_idx = seg->get_primary_key_index();
        int total = pk_idx->num_rows();
        uint32_t row_id = 0;
        int32_t remaining = total;
        bool exact_match = false;
        std::string last_key;
        int batch_size = 1024;
        MemPool pool;
        while (remaining > 0) {
            std::unique_ptr<segment_v2::IndexedColumnIterator> iter;
            RETURN_IF_ERROR(pk_idx->new_iterator(&iter));

            size_t num_to_read = std::min(batch_size, remaining);
            std::unique_ptr<ColumnVectorBatch> cvb;
            RETURN_IF_ERROR(ColumnVectorBatch::create(num_to_read, false, pk_idx->type_info(),
                                                      nullptr, &cvb));
            ColumnBlock block(cvb.get(), &pool);
            ColumnBlockView column_block_view(&block);
            Slice last_key_slice(last_key);
            RETURN_IF_ERROR(iter->seek_at_or_after(&last_key_slice, &exact_match));

            size_t num_read = num_to_read;
            RETURN_IF_ERROR(iter->next_batch(&num_read, &column_block_view));
            DCHECK(num_to_read == num_read);
            last_key = (reinterpret_cast<const Slice*>(cvb->cell_ptr(num_read - 1)))->to_string();

            // exclude last_key, last_key will be read in next batch.
            if (num_read == batch_size && num_read != remaining) {
                num_read -= 1;
            }
            for (size_t i = 0; i < num_read; i++) {
                const Slice* key = reinterpret_cast<const Slice*>(cvb->cell_ptr(i));
                RowLocation loc;
                // first check if exist in pre segment
                if (check_pre_segments) {
                    auto st = _check_pk_in_pre_segments(rowset_id, pre_segments, *key,
                                                        dummy_version, delete_bitmap, &loc);
                    if (st.ok()) {
                        delete_bitmap->add({rowset_id, loc.segment_id, dummy_version.first},
                                           loc.row_id);
                        ++row_id;
                        continue;
                    } else if (st.is<ALREADY_EXIST>()) {
                        delete_bitmap->add({rowset_id, seg->id(), dummy_version.first}, row_id);
                        ++row_id;
                        continue;
                    }
                }

                if (specified_rowset_ids != nullptr && !specified_rowset_ids->empty()) {
                    auto st = lookup_row_key(*key, specified_rowset_ids, &loc,
                                             dummy_version.first - 1);
                    CHECK(st.ok() || st.is<NOT_FOUND>() || st.is<ALREADY_EXIST>());
                    if (st.is<NOT_FOUND>()) {
                        ++row_id;
                        continue;
                    }

                    // sequence id smaller than the previous one, so delete current row
                    if (st.is<ALREADY_EXIST>()) {
                        loc.rowset_id = rowset_id;
                        loc.segment_id = seg->id();
                        loc.row_id = row_id;
                    }

                    delete_bitmap->add({loc.rowset_id, loc.segment_id, dummy_version.first},
                                       loc.row_id);
                }
                ++row_id;
            }
            remaining -= num_read;
        }
        if (check_pre_segments) {
            pre_segments.emplace_back(seg);
        }
    }
    LOG(INFO) << "construct delete bitmap tablet: " << tablet_id() << " rowset: " << rowset_id
              << " dummy_version: " << dummy_version
              << "bitmap num: " << delete_bitmap->delete_bitmap.size()
              << " cost: " << watch.get_elapse_time_us() << "(us)";
    return Status::OK();
}

Status Tablet::_check_pk_in_pre_segments(
        RowsetId rowset_id, const std::vector<segment_v2::SegmentSharedPtr>& pre_segments,
        const Slice& key, const Version& version, DeleteBitmapPtr delete_bitmap, RowLocation* loc) {
    for (auto it = pre_segments.rbegin(); it != pre_segments.rend(); ++it) {
        auto st = (*it)->lookup_row_key(key, loc);
        CHECK(st.ok() || st.is<NOT_FOUND>() || st.is<ALREADY_EXIST>());
        if (st.is<NOT_FOUND>()) {
            continue;
        } else if (st.ok() && _schema->has_sequence_col() &&
                   delete_bitmap->contains({rowset_id, loc->segment_id, version.first},
                                           loc->row_id)) {
            // if has sequence col, we continue to compare the sequence_id of
            // all segments, util we find an existing key.
            continue;
        }
        return st;
    }
    return Status::NotFound("Can't find key in the segment");
}

void Tablet::_rowset_ids_difference(const RowsetIdUnorderedSet& cur,
                                    const RowsetIdUnorderedSet& pre, RowsetIdUnorderedSet* to_add,
                                    RowsetIdUnorderedSet* to_del) {
    for (const auto& id : cur) {
        if (pre.find(id) == pre.end()) {
            to_add->insert(id);
        }
    }
    for (const auto& id : pre) {
        if (cur.find(id) == cur.end()) {
            to_del->insert(id);
        }
    }
}

// The caller should hold _rowset_update_lock and _meta_lock lock.
Status Tablet::update_delete_bitmap_without_lock(const RowsetSharedPtr& rowset) {
    int64_t cur_version = rowset->start_version();
    std::vector<segment_v2::SegmentSharedPtr> segments;
    _load_rowset_segments(rowset, &segments);

    DeleteBitmapPtr delete_bitmap = std::make_shared<DeleteBitmap>(tablet_id());
    RETURN_IF_ERROR(calc_delete_bitmap(rowset->rowset_id(), segments, nullptr, delete_bitmap,
                                       cur_version - 1, true));

    for (auto iter = delete_bitmap->delete_bitmap.begin();
         iter != delete_bitmap->delete_bitmap.end(); ++iter) {
        int ret = _tablet_meta->delete_bitmap().set(
                {std::get<0>(iter->first), std::get<1>(iter->first), cur_version}, iter->second);
        DCHECK(ret == 1);
    }

    return Status::OK();
}

Status Tablet::update_delete_bitmap(const RowsetSharedPtr& rowset, DeleteBitmapPtr delete_bitmap,
                                    const RowsetIdUnorderedSet& pre_rowset_ids) {
    RowsetIdUnorderedSet cur_rowset_ids;
    RowsetIdUnorderedSet rowset_ids_to_add;
    RowsetIdUnorderedSet rowset_ids_to_del;
    int64_t cur_version = rowset->start_version();

    std::vector<segment_v2::SegmentSharedPtr> segments;
    _load_rowset_segments(rowset, &segments);

    std::lock_guard<std::mutex> rwlock(_rowset_update_lock);
    std::shared_lock meta_rlock(_meta_lock);
    // tablet is under alter process. The delete bitmap will be calculated after conversion.
    if (tablet_state() == TABLET_NOTREADY &&
        SchemaChangeHandler::tablet_in_converting(tablet_id())) {
        LOG(INFO) << "tablet is under alter process, update delete bitmap later, tablet_id="
                  << tablet_id();
        return Status::OK();
    }
    cur_rowset_ids = all_rs_id(cur_version - 1);
    _rowset_ids_difference(cur_rowset_ids, pre_rowset_ids, &rowset_ids_to_add, &rowset_ids_to_del);
    if (!rowset_ids_to_add.empty() || !rowset_ids_to_del.empty()) {
        LOG(INFO) << "rowset_ids_to_add: " << rowset_ids_to_add.size()
                  << ", rowset_ids_to_del: " << rowset_ids_to_del.size();
    }
    for (const auto& to_del : rowset_ids_to_del) {
        delete_bitmap->remove({to_del, 0, 0}, {to_del, UINT32_MAX, INT64_MAX});
    }

    RETURN_IF_ERROR(calc_delete_bitmap(rowset->rowset_id(), segments, &rowset_ids_to_add,
                                       delete_bitmap, cur_version - 1, true));

    // update version without write lock, compaction and publish_txn
    // will update delete bitmap, handle compaction with _rowset_update_lock
    // and publish_txn runs sequential so no need to lock here
    for (auto iter = delete_bitmap->delete_bitmap.begin();
         iter != delete_bitmap->delete_bitmap.end(); ++iter) {
        int ret = _tablet_meta->delete_bitmap().set(
                {std::get<0>(iter->first), std::get<1>(iter->first), cur_version}, iter->second);
        DCHECK(ret == 1);
    }

    return Status::OK();
}

RowsetIdUnorderedSet Tablet::all_rs_id(int64_t max_version) const {
    RowsetIdUnorderedSet rowset_ids;
    auto end_it = _rowsets.upper_bound(max_version);
    for (auto it = _rowsets.begin(); it != end_it; ++it) {
        rowset_ids.insert(it->second->rowset_id());
    }
    return rowset_ids;
}

void Tablet::remove_self_owned_remote_rowsets() {
    DCHECK(tablet_state() == TABLET_SHUTDOWN);
    for (const auto& rs : _self_owned_remote_rowsets) {
        DCHECK(!rs->is_local());
        record_unused_remote_rowset(rs->rowset_id(), rs->rowset_meta()->resource_id(),
                                    rs->num_segments());
    }
}

void Tablet::update_self_owned_remote_rowsets(
        const std::vector<RowsetSharedPtr>& rowsets_in_snapshot) {
    if (_self_owned_remote_rowsets.empty()) {
        return;
    }
    for (const auto& rs : rowsets_in_snapshot) {
        if (!rs->is_local()) {
            auto it = _self_owned_remote_rowsets.find(rs);
            if (it != _self_owned_remote_rowsets.end()) {
                _self_owned_remote_rowsets.erase(it);
            }
        }
    }
}

bool Tablet::check_all_rowset_segment() {
    for (auto& [v, rs] : _rowsets) {
        if (!rs->check_rowset_segment()) {
            LOG(WARNING) << "Tablet Segment Check. find a bad tablet, tablet_id=" << tablet_id();
            return false;
        }
    }
    return true;
}

void Tablet::set_skip_compaction(bool skip, CompactionType compaction_type, int64_t start) {
    if (!skip) {
        _skip_cumu_compaction = false;
        _skip_base_compaction = false;
        return;
    }
    if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) {
        _skip_cumu_compaction = true;
        _skip_cumu_compaction_ts = start;
    } else {
        DCHECK(compaction_type == CompactionType::BASE_COMPACTION);
        _skip_base_compaction = true;
        _skip_base_compaction_ts = start;
    }
}

bool Tablet::should_skip_compaction(CompactionType compaction_type, int64_t now) {
    if (compaction_type == CompactionType::CUMULATIVE_COMPACTION && _skip_cumu_compaction &&
        now < _skip_cumu_compaction_ts + 120) {
        return true;
    } else if (compaction_type == CompactionType::BASE_COMPACTION && _skip_base_compaction &&
               now < _skip_base_compaction_ts + 120) {
        return true;
    }
    return false;
}

} // namespace doris

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

#include <atomic>
#include <cstddef>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

#include "common/logging.h"
#include "gen_cpp/olap_file.pb.h"
#include "olap/delete_handler.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowid_conversion.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/tablet_schema.h"
#include "util/uid_util.h"

namespace doris {

// Lifecycle states that a Tablet can be in. Legal state transitions for a
// Tablet object:
//
//   NOTREADY -> RUNNING -> TOMBSTONED -> STOPPED -> SHUTDOWN
//      |           |            |          ^^^
//      |           |            +----------++|
//      |           +------------------------+|
//      +-------------------------------------+

enum TabletState {
    // Tablet is under alter table, rollup, clone
    TABLET_NOTREADY,

    TABLET_RUNNING,

    // Tablet integrity has been violated, such as missing versions.
    // In this state, tablet will not accept any incoming request.
    // Report this state to FE, scheduling BE to drop tablet.
    TABLET_TOMBSTONED,

    // Tablet is shutting down, files in disk still remained.
    TABLET_STOPPED,

    // Files have been removed, tablet has been shutdown completely.
    TABLET_SHUTDOWN
};

class RowsetMeta;
class Rowset;
class DataDir;
class TabletMeta;
class DeleteBitmap;
using TabletMetaSharedPtr = std::shared_ptr<TabletMeta>;
using DeleteBitmapPtr = std::shared_ptr<DeleteBitmap>;

// Class encapsulates meta of tablet.
// The concurrency control is handled in Tablet Class, not in this class.
class TabletMeta {
public:
    static Status create(const TCreateTabletReq& request, const TabletUid& tablet_uid,
                         uint64_t shard_id, uint32_t next_unique_id,
                         const std::unordered_map<uint32_t, uint32_t>& col_ordinal_to_unique_id,
                         TabletMetaSharedPtr* tablet_meta);

    TabletMeta();
    // Only remote_storage_name is needed in meta, it is a key used to get remote params from fe.
    // The config of storage is saved in fe.
    TabletMeta(int64_t table_id, int64_t partition_id, int64_t tablet_id, int64_t replica_id,
               int32_t schema_hash, uint64_t shard_id, const TTabletSchema& tablet_schema,
               uint32_t next_unique_id,
               const std::unordered_map<uint32_t, uint32_t>& col_ordinal_to_unique_id,
               TabletUid tablet_uid, TTabletType::type tabletType,
               TCompressionType::type compression_type,
               const std::string& storage_policy = std::string(),
               bool enable_unique_key_merge_on_write = false);
    // If need add a filed in TableMeta, filed init copy in copy construct function
    TabletMeta(const TabletMeta& tablet_meta);
    TabletMeta(TabletMeta&& tablet_meta) = delete;

    // Function create_from_file is used to be compatible with previous tablet_meta.
    // Previous tablet_meta is a physical file in tablet dir, which is not stored in rocksdb.
    Status create_from_file(const std::string& file_path);
    Status save(const std::string& file_path);
    Status save_as_json(const string& file_path, DataDir* dir);
    static Status save(const std::string& file_path, const TabletMetaPB& tablet_meta_pb);
    static Status reset_tablet_uid(const std::string& file_path);
    static std::string construct_header_file_path(const std::string& schema_hash_path,
                                                  int64_t tablet_id);
    Status save_meta(DataDir* data_dir);

    void init_from_pb(const TabletMetaPB& tablet_meta_pb);

    void to_meta_pb(TabletMetaPB* tablet_meta_pb, const std::vector<RowsetMetaSharedPtr>& rs_metas);
    void to_json(std::string* json_string, json2pb::Pb2JsonOptions& options);
    uint32_t mem_size() const;

    TabletTypePB tablet_type() const { return _tablet_type; }
    TabletUid tablet_uid() const;
    int64_t table_id() const;
    int64_t partition_id() const;
    int64_t tablet_id() const;
    int64_t replica_id() const;
    int32_t schema_hash() const;
    int16_t shard_id() const;
    void set_shard_id(int32_t shard_id);
    int64_t creation_time() const;
    void set_creation_time(int64_t creation_time);
    int64_t cumulative_layer_point() const;
    void set_cumulative_layer_point(int64_t new_point);

    TabletState tablet_state() const;
    void set_tablet_state(TabletState state);

    bool in_restore_mode() const;
    void set_in_restore_mode(bool in_restore_mode);

    TabletSchemaSPtr tablet_schema() const;

    const TabletSchemaSPtr tablet_schema(Version version) const;

    TabletSchema* mutable_tablet_schema();

    void revise_delete_bitmap_unlocked(const DeleteBitmap& delete_bitmap,
                                       const std::vector<RowsetMetaSharedPtr>& rs_metas,
                                       const std::vector<RowsetMetaSharedPtr>& stale_rs_metas);

    std::string full_name() const;

    Status set_partition_id(int64_t partition_id);

    RowsetTypePB preferred_rowset_type() const { return _preferred_rowset_type; }

    void set_preferred_rowset_type(RowsetTypePB preferred_rowset_type) {
        _preferred_rowset_type = preferred_rowset_type;
    }

    const std::string& storage_policy() const {
        std::shared_lock<std::shared_mutex> rlock(_meta_lock);
        return _storage_policy;
    }

    void set_storage_policy(const std::string& policy) {
        std::unique_lock<std::shared_mutex> wlock(_meta_lock);
        VLOG_NOTICE << "set tablet_id : " << _table_id << " storage policy from " << _storage_policy
                    << " to " << policy;
        _storage_policy = policy;
    }

    static void init_column_from_tcolumn(uint32_t unique_id, const TColumn& tcolumn,
                                         ColumnPB* column);

    DeleteBitmap& delete_bitmap() { return *_delete_bitmap; }

    bool enable_unique_key_merge_on_write() const { return _enable_unique_key_merge_on_write; }

    void update_delete_bitmap(const std::vector<RowsetSharedPtr>& input_rowsets,
                              const Version& version, const RowIdConversion& rowid_conversion);

private:
    Status _save_meta(DataDir* data_dir);

    // _del_predicates is ignored to compare.
    friend bool operator==(const TabletMeta& a, const TabletMeta& b);
    friend bool operator!=(const TabletMeta& a, const TabletMeta& b);

private:
    int64_t _table_id = 0;
    int64_t _partition_id = 0;
    int64_t _tablet_id = 0;
    int64_t _replica_id = 0;
    int32_t _schema_hash = 0;
    int32_t _shard_id = 0;
    int64_t _creation_time = 0;
    int64_t _cumulative_layer_point = 0;
    TabletUid _tablet_uid;
    TabletTypePB _tablet_type = TabletTypePB::TABLET_TYPE_DISK;

    TabletState _tablet_state = TABLET_NOTREADY;
    // the reference of _schema may use in tablet, so here need keep
    // the lifetime of tablemeta and _schema is same with tablet
    TabletSchemaSPtr _schema;

    bool _in_restore_mode = false;
    RowsetTypePB _preferred_rowset_type = BETA_ROWSET;

    std::string _storage_policy;

    // For unique key data model, the feature Merge-on-Write will leverage a primary
    // key index and a delete-bitmap to mark duplicate keys as deleted in load stage,
    // which can avoid the merging cost in read stage, and accelerate the aggregation
    // query performance significantly.
    bool _enable_unique_key_merge_on_write = false;
    std::shared_ptr<DeleteBitmap> _delete_bitmap;

    mutable std::shared_mutex _meta_lock;
};

/**
 * Wraps multiple bitmaps for recording rows (row id) that are deleted or
 * overwritten. For now, it's only used when unique key merge-on-write property
 * enabled.
 *
 * RowsetId and SegmentId are for locating segment, Version here is a single
 * uint32_t means that at which "version" of the load causes the delete or
 * overwrite.
 *
 * The start and end version of a load is the same, it's ok and straightforward
 * to use a single uint32_t.
 *
 * e.g.
 * There is a key "key1" in rowset id 1, version [1,1], segment id 1, row id 1.
 * A new load also contains "key1", the rowset id 2, version [2,2], segment id 1
 * the delete bitmap will be `{1,1,2} -> 1`, which means the "row id 1" in
 * "rowset id 1, segment id 1" is deleted/overitten by some loads at "version 2"
 */
class DeleteBitmap {
public:
    mutable std::shared_mutex lock;
    using SegmentId = uint32_t;
    using Version = uint64_t;
    using BitmapKey = std::tuple<RowsetId, SegmentId, Version>;
    std::map<BitmapKey, roaring::Roaring> delete_bitmap; // Ordered map

    /**
     * 
     * @param tablet_id the tablet which this delete bitmap associates with
     */
    DeleteBitmap(int64_t tablet_id);

    /**
     * Copy c-tor for making delete bitmap snapshot on read path
     */
    DeleteBitmap(const DeleteBitmap& r);
    DeleteBitmap& operator=(const DeleteBitmap& r);
    /**
     * Move c-tor for making delete bitmap snapshot on read path
     */
    DeleteBitmap(DeleteBitmap&& r);
    DeleteBitmap& operator=(DeleteBitmap&& r);

    /**
     * Makes a snapshot of delete bimap, read lock will be acquired in this
     * process
     */
    DeleteBitmap snapshot() const;

    /**
     * Makes a snapshot of delete bimap on given version, read lock will be
     * acquired temporary in this process
     */
    DeleteBitmap snapshot(Version version) const;

    /**
     * Marks the specific row deleted
     */
    void add(const BitmapKey& bmk, uint32_t row_id);

    /**
     * Clears the deletetion mark specific row
     *
     * @return non-zero if the associated delete bimap does not exist
     */
    int remove(const BitmapKey& bmk, uint32_t row_id);

    /**
     * Clears bitmaps in range [lower_key, upper_key)
     */
    void remove(const BitmapKey& lower_key, const BitmapKey& upper_key);

    /**
     * Checks if the given row is marked deleted
     *
     * @return true if marked deleted
     */
    bool contains(const BitmapKey& bmk, uint32_t row_id) const;

    /**
     * Sets the bitmap of specific segment, it's may be insertion or replacement
     *
     * @return 1 if the insertion took place, 0 if the assignment took place
     */
    int set(const BitmapKey& bmk, const roaring::Roaring& segment_delete_bitmap);

    /**
     * Gets a copy of specific delete bmk
     *
     * @param segment_delete_bitmap output param
     * @return non-zero if the associated delete bimap does not exist
     */
    int get(const BitmapKey& bmk, roaring::Roaring* segment_delete_bitmap) const;

    /**
     * Gets reference to a specific delete map, DO NOT use this function on a
     * mutable DeleteBitmap object
     * @return nullptr if the given bitmap does not exist
     */
    const roaring::Roaring* get(const BitmapKey& bmk) const;

    /**
     * Gets subset of delete_bitmap with given range [start, end)
     *
     * @parma start start
     * @parma end end
     * @parma subset_delete_map output param
     */
    void subset(const BitmapKey& start, const BitmapKey& end,
                DeleteBitmap* subset_delete_map) const;

    /**
     * Merges the given delete bitmap into *this
     *
     * @param other
     */
    void merge(const DeleteBitmap& other);

    /**
     * Checks if the given row is marked deleted in bitmap with the condition:
     * all the bitmaps that
     * RowsetId and SegmentId are the same as the given ones,
     * and Version <= the given Version
     *
     * Note: aggregation cache may be used.
     *
     * @return true if marked deleted
     */
    bool contains_agg(const BitmapKey& bitmap, uint32_t row_id) const;

    /**
     * Gets aggregated delete_bitmap on rowset_id and version, the same effect:
     * `select sum(roaring::Roaring) where RowsetId=rowset_id and SegmentId=seg_id and Version <= version`
     *
     * @return shared_ptr to a bitmap, which may be empty
     */
    std::shared_ptr<roaring::Roaring> get_agg(const BitmapKey& bmk) const;

    class AggCache {
    public:
        struct Value {
            roaring::Roaring bitmap;
        };

        AggCache(size_t size_in_bytes) {
            static std::once_flag once;
            std::call_once(once, [size_in_bytes] {
                auto tmp = new ShardedLRUCache("DeleteBitmap AggCache", size_in_bytes,
                                               LRUCacheType::SIZE, 256);
                AggCache::s_repr.store(tmp, std::memory_order_release);
            });

            while (!s_repr.load(std::memory_order_acquire)) {
            }
        }

        static ShardedLRUCache* repr() { return s_repr.load(std::memory_order_acquire); }
        static std::atomic<ShardedLRUCache*> s_repr;
    };

private:
    mutable std::shared_ptr<AggCache> _agg_cache;
    int64_t _tablet_id;
};

static const std::string SEQUENCE_COL = "__DORIS_SEQUENCE_COL__";

inline TabletUid TabletMeta::tablet_uid() const {
    return _tablet_uid;
}

inline int64_t TabletMeta::table_id() const {
    return _table_id;
}

inline int64_t TabletMeta::partition_id() const {
    return _partition_id;
}

inline int64_t TabletMeta::tablet_id() const {
    return _tablet_id;
}

inline int64_t TabletMeta::replica_id() const {
    return _replica_id;
}

inline int32_t TabletMeta::schema_hash() const {
    return _schema_hash;
}

inline int16_t TabletMeta::shard_id() const {
    return _shard_id;
}

inline void TabletMeta::set_shard_id(int32_t shard_id) {
    _shard_id = shard_id;
}

inline int64_t TabletMeta::creation_time() const {
    return _creation_time;
}

inline void TabletMeta::set_creation_time(int64_t creation_time) {
    _creation_time = creation_time;
}

inline int64_t TabletMeta::cumulative_layer_point() const {
    return _cumulative_layer_point;
}

inline void TabletMeta::set_cumulative_layer_point(int64_t new_point) {
    _cumulative_layer_point = new_point;
}

inline TabletState TabletMeta::tablet_state() const {
    return _tablet_state;
}

inline void TabletMeta::set_tablet_state(TabletState state) {
    _tablet_state = state;
}

inline bool TabletMeta::in_restore_mode() const {
    return _in_restore_mode;
}

inline void TabletMeta::set_in_restore_mode(bool in_restore_mode) {
    _in_restore_mode = in_restore_mode;
}

inline TabletSchemaSPtr TabletMeta::tablet_schema() const {
    return _schema;
}

inline TabletSchema* TabletMeta::mutable_tablet_schema() {
    return _schema.get();
}

// Only for unit test now.
bool operator==(const TabletMeta& a, const TabletMeta& b);
bool operator!=(const TabletMeta& a, const TabletMeta& b);

} // namespace doris

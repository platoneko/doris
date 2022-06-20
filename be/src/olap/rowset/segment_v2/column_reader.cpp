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

#include "olap/rowset/segment_v2/column_reader.h"

#include "gutil/strings/substitute.h"                // for Substitute
#include "olap/column_block.h"                       // for ColumnBlockView
#include "olap/rowset/segment_v2/binary_dict_page.h" // for BinaryDictPageDecoder
#include "olap/rowset/segment_v2/bloom_filter_index_reader.h"
#include "olap/rowset/segment_v2/encoding_info.h" // for EncodingInfo
#include "olap/rowset/segment_v2/page_handle.h"   // for PageHandle
#include "olap/rowset/segment_v2/page_io.h"
#include "olap/rowset/segment_v2/page_pointer.h" // for PagePointer
#include "olap/types.h"                          // for TypeInfo
#include "util/block_compression.h"
#include "util/rle_encoding.h" // for RleDecoder
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/core/types.h"
#include "vec/runtime/vdatetime_value.h" //for VecDateTime

namespace doris {
namespace segment_v2 {

using strings::Substitute;

Status ColumnReader::create(const ColumnReaderOptions& opts, const ColumnMetaPB& meta,
                            uint64_t num_rows, io::FileSystem* fs, const std::string& path,
                            std::unique_ptr<ColumnReader>* reader) {
    if (is_scalar_type((FieldType)meta.type())) {
        std::unique_ptr<ColumnReader> reader_local(
                new ColumnReader(opts, meta, num_rows, fs, path));
        RETURN_IF_ERROR(reader_local->init());
        *reader = std::move(reader_local);
        return Status::OK();
    } else {
        auto type = (FieldType)meta.type();
        switch (type) {
        case FieldType::OLAP_FIELD_TYPE_ARRAY: {
            DCHECK(meta.children_columns_size() == 2 || meta.children_columns_size() == 3);

            std::unique_ptr<ColumnReader> item_reader;
            RETURN_IF_ERROR(ColumnReader::create(opts, meta.children_columns(0),
                                                 meta.children_columns(0).num_rows(), fs, path,
                                                 &item_reader));

            std::unique_ptr<ColumnReader> offset_reader;
            RETURN_IF_ERROR(ColumnReader::create(opts, meta.children_columns(1),
                                                 meta.children_columns(1).num_rows(), fs, path,
                                                 &offset_reader));

            std::unique_ptr<ColumnReader> null_reader;
            if (meta.is_nullable()) {
                RETURN_IF_ERROR(ColumnReader::create(opts, meta.children_columns(2),
                                                     meta.children_columns(2).num_rows(), fs, path,
                                                     &null_reader));
            }

            // The num rows of the array reader equals to the num rows of the length reader.
            num_rows = meta.children_columns(1).num_rows();
            std::unique_ptr<ColumnReader> array_reader(
                    new ColumnReader(opts, meta, num_rows, fs, path));
            //  array reader do not need to init
            array_reader->_sub_readers.resize(meta.children_columns_size());
            array_reader->_sub_readers[0] = std::move(item_reader);
            array_reader->_sub_readers[1] = std::move(offset_reader);
            if (meta.is_nullable()) {
                array_reader->_sub_readers[2] = std::move(null_reader);
            }
            *reader = std::move(array_reader);
            return Status::OK();
        }
        default:
            return Status::NotSupported("unsupported type for ColumnReader: {}",
                                        std::to_string(type));
        }
    }
}

ColumnReader::ColumnReader(const ColumnReaderOptions& opts, const ColumnMetaPB& meta,
                           uint64_t num_rows, io::FileSystem* fs, const std::string& path)
        : _meta(meta),
          _opts(opts),
          _num_rows(num_rows),
          _fs(fs),
          _path(path),
          _dict_encoding_type(UNKNOWN_DICT_ENCODING) {}

ColumnReader::~ColumnReader() = default;

Status ColumnReader::init() {
    _type_info = get_type_info(&_meta);
    if (_type_info == nullptr) {
        return Status::NotSupported("unsupported typeinfo, type={}", _meta.type());
    }
    RETURN_IF_ERROR(EncodingInfo::get(_type_info.get(), _meta.encoding(), &_encoding_info));

    for (int i = 0; i < _meta.indexes_size(); i++) {
        auto& index_meta = _meta.indexes(i);
        switch (index_meta.type()) {
        case ORDINAL_INDEX:
            _ordinal_index_meta = &index_meta.ordinal_index();
            break;
        case ZONE_MAP_INDEX:
            _zone_map_index_meta = &index_meta.zone_map_index();
            break;
        case BITMAP_INDEX:
            _bitmap_index_meta = &index_meta.bitmap_index();
            break;
        case BLOOM_FILTER_INDEX:
            _bf_index_meta = &index_meta.bloom_filter_index();
            break;
        default:
            return Status::Corruption("Bad file {}: invalid column index type {}", _path,
                                      index_meta.type());
        }
    }
    // ArrayColumnWriter writes a single empty array and flushes. In this scenario,
    // the item writer doesn't write any data and the corresponding ordinal index is empty.
    if (_ordinal_index_meta == nullptr && !is_empty()) {
        return Status::Corruption("Bad file {}: missing ordinal index for column {}", _path,
                                  _meta.column_id());
    }
    return Status::OK();
}

Status ColumnReader::new_bitmap_index_iterator(BitmapIndexIterator** iterator) {
    RETURN_IF_ERROR(_ensure_index_loaded());
    RETURN_IF_ERROR(_bitmap_index->new_iterator(iterator));
    return Status::OK();
}

Status ColumnReader::read_page(const ColumnIteratorOptions& iter_opts, const PagePointer& pp,
                               PageHandle* handle, Slice* page_body, PageFooterPB* footer,
                               BlockCompressionCodec* codec) const {
    iter_opts.sanity_check();
    PageReadOptions opts;
    opts.file_reader = iter_opts.file_reader;
    opts.page_pointer = pp;
    opts.codec = codec;
    opts.stats = iter_opts.stats;
    opts.verify_checksum = _opts.verify_checksum;
    opts.use_page_cache = iter_opts.use_page_cache;
    opts.kept_in_memory = _opts.kept_in_memory;
    opts.type = iter_opts.type;
    opts.encoding_info = _encoding_info;

    return PageIO::read_and_decompress_page(opts, handle, page_body, footer);
}

Status ColumnReader::get_row_ranges_by_zone_map(CondColumn* cond_column,
                                                CondColumn* delete_condition,
                                                RowRanges* row_ranges) {
    RETURN_IF_ERROR(_ensure_index_loaded());

    std::vector<uint32_t> page_indexes;
    RETURN_IF_ERROR(_get_filtered_pages(cond_column, delete_condition, &page_indexes));
    RETURN_IF_ERROR(_calculate_row_ranges(page_indexes, row_ranges));
    return Status::OK();
}

bool ColumnReader::match_condition(CondColumn* cond) const {
    if (_zone_map_index_meta == nullptr || cond == nullptr) {
        return true;
    }
    FieldType type = _type_info->type();
    std::unique_ptr<WrapperField> min_value(WrapperField::create_by_type(type, _meta.length()));
    std::unique_ptr<WrapperField> max_value(WrapperField::create_by_type(type, _meta.length()));
    _parse_zone_map(_zone_map_index_meta->segment_zone_map(), min_value.get(), max_value.get());

    return _zone_map_match_condition(_zone_map_index_meta->segment_zone_map(), min_value.get(),
                                     max_value.get(), cond);
}

void ColumnReader::_parse_zone_map(const ZoneMapPB& zone_map, WrapperField* min_value_container,
                                   WrapperField* max_value_container) const {
    // min value and max value are valid if has_not_null is true
    if (zone_map.has_not_null()) {
        min_value_container->from_string(zone_map.min());
        max_value_container->from_string(zone_map.max());
    }
    // for compatible original Cond eval logic
    // TODO(hkp): optimize OlapCond
    if (zone_map.has_null()) {
        // for compatible, if exist null, original logic treat null as min
        min_value_container->set_null();
        if (!zone_map.has_not_null()) {
            // for compatible OlapCond's 'is not null'
            max_value_container->set_null();
        }
    }
}

bool ColumnReader::_zone_map_match_condition(const ZoneMapPB& zone_map,
                                             WrapperField* min_value_container,
                                             WrapperField* max_value_container,
                                             CondColumn* cond) const {
    if (!zone_map.has_not_null() && !zone_map.has_null()) {
        return false; // no data in this zone
    }

    if (cond == nullptr || zone_map.pass_all()) {
        return true;
    }

    return cond->eval({min_value_container, max_value_container});
}

Status ColumnReader::_get_filtered_pages(CondColumn* cond_column, CondColumn* delete_condition,
                                         std::vector<uint32_t>* page_indexes) {
    FieldType type = _type_info->type();
    const std::vector<ZoneMapPB>& zone_maps = _zone_map_index->page_zone_maps();
    int32_t page_size = _zone_map_index->num_pages();
    std::unique_ptr<WrapperField> min_value(WrapperField::create_by_type(type, _meta.length()));
    std::unique_ptr<WrapperField> max_value(WrapperField::create_by_type(type, _meta.length()));
    for (int32_t i = 0; i < page_size; ++i) {
        if (zone_maps[i].pass_all()) {
            page_indexes->push_back(i);
        } else {
            _parse_zone_map(zone_maps[i], min_value.get(), max_value.get());
            if (_zone_map_match_condition(zone_maps[i], min_value.get(), max_value.get(),
                                          cond_column)) {
                bool should_read = true;
                if (delete_condition != nullptr) {
                    int state = delete_condition->del_eval({min_value.get(), max_value.get()});
                    if (state == DEL_SATISFIED) {
                        should_read = false;
                    }
                }
                if (should_read) {
                    page_indexes->push_back(i);
                }
            }
        }
    }
    VLOG(1) << "total-pages: " << page_size << " not-filtered-pages: " << page_indexes->size()
            << " filtered-percent:" << 1.0 - (page_indexes->size() * 1.0) / (page_size * 1.0);
    return Status::OK();
}

Status ColumnReader::_calculate_row_ranges(const std::vector<uint32_t>& page_indexes,
                                           RowRanges* row_ranges) {
    row_ranges->clear();
    for (auto i : page_indexes) {
        ordinal_t page_first_id = _ordinal_index->get_first_ordinal(i);
        ordinal_t page_last_id = _ordinal_index->get_last_ordinal(i);
        RowRanges page_row_ranges(RowRanges::create_single(page_first_id, page_last_id + 1));
        RowRanges::ranges_union(*row_ranges, page_row_ranges, row_ranges);
    }
    return Status::OK();
}

Status ColumnReader::get_row_ranges_by_bloom_filter(CondColumn* cond_column,
                                                    RowRanges* row_ranges) {
    RETURN_IF_ERROR(_ensure_index_loaded());
    RowRanges bf_row_ranges;
    std::unique_ptr<BloomFilterIndexIterator> bf_iter;
    RETURN_IF_ERROR(_bloom_filter_index->new_iterator(&bf_iter));
    size_t range_size = row_ranges->range_size();
    // get covered page ids
    std::set<uint32_t> page_ids;
    for (int i = 0; i < range_size; ++i) {
        int64_t from = row_ranges->get_range_from(i);
        int64_t idx = from;
        int64_t to = row_ranges->get_range_to(i);
        auto iter = _ordinal_index->seek_at_or_before(from);
        while (idx < to && iter.valid()) {
            page_ids.insert(iter.page_index());
            idx = iter.last_ordinal() + 1;
            iter.next();
        }
    }
    for (auto& pid : page_ids) {
        std::unique_ptr<BloomFilter> bf;
        RETURN_IF_ERROR(bf_iter->read_bloom_filter(pid, &bf));
        if (cond_column->eval(bf.get())) {
            bf_row_ranges.add(RowRange(_ordinal_index->get_first_ordinal(pid),
                                       _ordinal_index->get_last_ordinal(pid) + 1));
        }
    }
    RowRanges::ranges_intersection(*row_ranges, bf_row_ranges, row_ranges);
    return Status::OK();
}

Status ColumnReader::_load_ordinal_index(bool use_page_cache, bool kept_in_memory) {
    DCHECK(_ordinal_index_meta != nullptr);
    _ordinal_index.reset(new OrdinalIndexReader(_fs, _path, _ordinal_index_meta, _num_rows));
    return _ordinal_index->load(use_page_cache, kept_in_memory);
}

Status ColumnReader::_load_zone_map_index(bool use_page_cache, bool kept_in_memory) {
    if (_zone_map_index_meta != nullptr) {
        _zone_map_index.reset(new ZoneMapIndexReader(_fs, _path, _zone_map_index_meta));
        return _zone_map_index->load(use_page_cache, kept_in_memory);
    }
    return Status::OK();
}

Status ColumnReader::_load_bitmap_index(bool use_page_cache, bool kept_in_memory) {
    if (_bitmap_index_meta != nullptr) {
        _bitmap_index.reset(new BitmapIndexReader(_fs, _path, _bitmap_index_meta));
        return _bitmap_index->load(use_page_cache, kept_in_memory);
    }
    return Status::OK();
}

Status ColumnReader::_load_bloom_filter_index(bool use_page_cache, bool kept_in_memory) {
    if (_bf_index_meta != nullptr) {
        _bloom_filter_index.reset(new BloomFilterIndexReader(_fs, _path, _bf_index_meta));
        return _bloom_filter_index->load(use_page_cache, kept_in_memory);
    }
    return Status::OK();
}

Status ColumnReader::seek_to_first(OrdinalPageIndexIterator* iter) {
    RETURN_IF_ERROR(_ensure_index_loaded());
    *iter = _ordinal_index->begin();
    if (!iter->valid()) {
        return Status::NotFound("Failed to seek to first rowid");
    }
    return Status::OK();
}

Status ColumnReader::seek_at_or_before(ordinal_t ordinal, OrdinalPageIndexIterator* iter) {
    RETURN_IF_ERROR(_ensure_index_loaded());
    *iter = _ordinal_index->seek_at_or_before(ordinal);
    if (!iter->valid()) {
        return Status::NotFound("Failed to seek to ordinal {}, ", ordinal);
    }
    return Status::OK();
}

Status ColumnReader::new_iterator(ColumnIterator** iterator) {
    if (is_empty()) {
        *iterator = new EmptyFileColumnIterator();
        return Status::OK();
    }
    if (is_scalar_type((FieldType)_meta.type())) {
        *iterator = new FileColumnIterator(this);
        return Status::OK();
    } else {
        auto type = (FieldType)_meta.type();
        switch (type) {
        case FieldType::OLAP_FIELD_TYPE_ARRAY: {
            ColumnIterator* item_iterator = nullptr;
            RETURN_IF_ERROR(_sub_readers[0]->new_iterator(&item_iterator));

            ColumnIterator* offset_iterator = nullptr;
            RETURN_IF_ERROR(_sub_readers[1]->new_iterator(&offset_iterator));

            ColumnIterator* null_iterator = nullptr;
            if (is_nullable()) {
                RETURN_IF_ERROR(_sub_readers[2]->new_iterator(&null_iterator));
            }
            *iterator = new ArrayFileColumnIterator(
                    this, reinterpret_cast<FileColumnIterator*>(offset_iterator), item_iterator,
                    null_iterator);
            return Status::OK();
        }
        default:
            return Status::NotSupported("unsupported type to create iterator: {}",
                                        std::to_string(type));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

ArrayFileColumnIterator::ArrayFileColumnIterator(ColumnReader* reader,
                                                 FileColumnIterator* offset_reader,
                                                 ColumnIterator* item_iterator,
                                                 ColumnIterator* null_iterator)
        : _array_reader(reader) {
    _length_iterator.reset(offset_reader);
    _item_iterator.reset(item_iterator);
    if (_array_reader->is_nullable()) {
        _null_iterator.reset(null_iterator);
    }
}

Status ArrayFileColumnIterator::init(const ColumnIteratorOptions& opts) {
    RETURN_IF_ERROR(_length_iterator->init(opts));
    RETURN_IF_ERROR(_item_iterator->init(opts));
    if (_array_reader->is_nullable()) {
        RETURN_IF_ERROR(_null_iterator->init(opts));
    }
    const auto* offset_type_info = get_scalar_type_info<OLAP_FIELD_TYPE_UNSIGNED_BIGINT>();
    RETURN_IF_ERROR(
            ColumnVectorBatch::create(1024, false, offset_type_info, nullptr, &_length_batch));
    return Status::OK();
}

Status ArrayFileColumnIterator::next_batch(size_t* n, ColumnBlockView* dst, bool* has_null) {
    ColumnBlock* array_block = dst->column_block();
    auto* array_batch = static_cast<ArrayColumnVectorBatch*>(array_block->vector_batch());

    // 1. read n offsets
    ColumnBlock offset_block(array_batch->offsets(), nullptr);
    ColumnBlockView offset_view(&offset_block,
                                dst->current_offset() + 1); // offset应该比collection的游标多1
    bool offset_has_null = false;
    RETURN_IF_ERROR(_length_iterator->next_batch(n, &offset_view, &offset_has_null));
    DCHECK(!offset_has_null);

    if (*n == 0) {
        return Status::OK();
    }
    array_batch->get_offset_by_length(dst->current_offset(), *n);

    // 2. read null
    if (_array_reader->is_nullable()) {
        DCHECK(dst->is_nullable());
        auto null_batch = array_batch->get_null_as_batch();
        ColumnBlock null_block(&null_batch, nullptr);
        ColumnBlockView null_view(&null_block, dst->current_offset());
        size_t size = *n;
        bool null_signs_has_null = false;
        _null_iterator->next_batch(&size, &null_view, &null_signs_has_null);
        DCHECK(!null_signs_has_null);
        *has_null = true; // just set has_null to is_nullable
    } else {
        *has_null = false;
    }

    // read item
    size_t item_size = array_batch->get_item_size(dst->current_offset(), *n);
    if (item_size >= 0) {
        bool item_has_null = false;
        ColumnVectorBatch* item_vector_batch = array_batch->elements();

        bool rebuild_array_from0 = false;
        if (item_vector_batch->capacity() < array_batch->item_offset(dst->current_offset() + *n)) {
            item_vector_batch->resize(array_batch->item_offset(dst->current_offset() + *n));
            rebuild_array_from0 = true;
        }

        ColumnBlock item_block = ColumnBlock(item_vector_batch, dst->pool());
        ColumnBlockView item_view =
                ColumnBlockView(&item_block, array_batch->item_offset(dst->current_offset()));
        size_t real_read = item_size;
        RETURN_IF_ERROR(_item_iterator->next_batch(&real_read, &item_view, &item_has_null));
        DCHECK(item_size == real_read);

        size_t rebuild_start_offset = rebuild_array_from0 ? 0 : dst->current_offset();
        size_t rebuild_size = rebuild_array_from0 ? dst->current_offset() + *n : *n;
        array_batch->prepare_for_read(rebuild_start_offset, rebuild_size, item_has_null);
    }

    dst->advance(*n);
    return Status::OK();
}

Status ArrayFileColumnIterator::next_batch(size_t* n, vectorized::MutableColumnPtr& dst,
                                           bool* has_null) {
    const auto* column_array = vectorized::check_and_get_column<vectorized::ColumnArray>(
            dst->is_nullable() ? static_cast<vectorized::ColumnNullable&>(*dst).get_nested_column()
                               : *dst);

    bool offsets_has_null = false;
    auto column_offsets_ptr = column_array->get_offsets_column().assume_mutable();
    ssize_t start = column_offsets_ptr->size();
    RETURN_IF_ERROR(_length_iterator->next_batch(n, column_offsets_ptr, &offsets_has_null));
    if (*n == 0) {
        return Status::OK();
    }
    auto& column_offsets =
            static_cast<vectorized::ColumnArray::ColumnOffsets&>(*column_offsets_ptr);
    auto& offsets_data = column_offsets.get_data();
    for (ssize_t i = start; i < offsets_data.size(); ++i) {
        offsets_data[i] += offsets_data[i - 1]; // -1 is ok
    }

    auto column_items_ptr = column_array->get_data().assume_mutable();
    size_t num_items = offsets_data.back() - offsets_data[start - 1];
    if (num_items > 0) {
        size_t num_read = num_items;
        bool items_has_null = false;
        RETURN_IF_ERROR(_item_iterator->next_batch(&num_read, column_items_ptr, &items_has_null));
        DCHECK(num_read == num_items);
    }

    if (dst->is_nullable()) {
        auto null_map_ptr =
                static_cast<vectorized::ColumnNullable&>(*dst).get_null_map_column_ptr();
        size_t num_read = *n;
        bool null_signs_has_null = false;
        RETURN_IF_ERROR(_null_iterator->next_batch(&num_read, null_map_ptr, &null_signs_has_null));
        DCHECK(num_read == *n);
    }

    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////

FileColumnIterator::FileColumnIterator(ColumnReader* reader) : _reader(reader) {}

Status FileColumnIterator::init(const ColumnIteratorOptions& opts) {
    _opts = opts;
    RETURN_IF_ERROR(get_block_compression_codec(_reader->get_compression(), _compress_codec));
    if (config::enable_low_cardinality_optimize &&
        _reader->encoding_info()->encoding() == DICT_ENCODING) {
        auto dict_encoding_type = _reader->get_dict_encoding_type();
        if (dict_encoding_type == ColumnReader::UNKNOWN_DICT_ENCODING) {
            seek_to_ordinal(_reader->num_rows() - 1);
            _is_all_dict_encoding = _page.is_dict_encoding;
            _reader->set_dict_encoding_type(_is_all_dict_encoding
                                                    ? ColumnReader::ALL_DICT_ENCODING
                                                    : ColumnReader::PARTIAL_DICT_ENCODING);
        } else {
            _is_all_dict_encoding = dict_encoding_type == ColumnReader::ALL_DICT_ENCODING;
        }
    }
    return Status::OK();
}

FileColumnIterator::~FileColumnIterator() = default;

Status FileColumnIterator::seek_to_first() {
    RETURN_IF_ERROR(_reader->seek_to_first(&_page_iter));
    RETURN_IF_ERROR(_read_data_page(_page_iter));

    _seek_to_pos_in_page(&_page, 0);
    _current_ordinal = 0;
    return Status::OK();
}

Status FileColumnIterator::seek_to_ordinal(ordinal_t ord) {
    // if current page contains this row, we don't need to seek
    if (!_page || !_page.contains(ord) || !_page_iter.valid()) {
        RETURN_IF_ERROR(_reader->seek_at_or_before(ord, &_page_iter));
        RETURN_IF_ERROR(_read_data_page(_page_iter));
    }
    _seek_to_pos_in_page(&_page, ord - _page.first_ordinal);
    _current_ordinal = ord;
    return Status::OK();
}

Status FileColumnIterator::seek_to_page_start() {
    return seek_to_ordinal(_page.first_ordinal);
}

void FileColumnIterator::_seek_to_pos_in_page(ParsedPage* page, ordinal_t offset_in_page) const {
    if (page->offset_in_page == offset_in_page) {
        // fast path, do nothing
        return;
    }

    ordinal_t pos_in_data = offset_in_page;
    if (_page.has_null) {
        ordinal_t offset_in_data = 0;
        ordinal_t skips = offset_in_page;

        if (offset_in_page > page->offset_in_page) {
            // forward, reuse null bitmap
            skips = offset_in_page - page->offset_in_page;
            offset_in_data = page->data_decoder->current_index();
        } else {
            // rewind null bitmap, and
            page->null_decoder = RleDecoder<bool>((const uint8_t*)page->null_bitmap.data,
                                                  page->null_bitmap.size, 1);
        }

        auto skip_nulls = page->null_decoder.Skip(skips);
        pos_in_data = offset_in_data + skips - skip_nulls;
    }

    page->data_decoder->seek_to_position_in_page(pos_in_data);
    page->offset_in_page = offset_in_page;
}

Status FileColumnIterator::next_batch(size_t* n, ColumnBlockView* dst, bool* has_null) {
    size_t remaining = *n;
    *has_null = false;
    while (remaining > 0) {
        if (!_page.has_remaining()) {
            bool eos = false;
            RETURN_IF_ERROR(_load_next_page(&eos));
            if (eos) {
                break;
            }
        }

        // number of rows to be read from this page
        size_t nrows_in_page = std::min(remaining, _page.remaining());
        size_t nrows_to_read = nrows_in_page;
        if (_page.has_null) {
            // when this page contains NULLs we read data in some runs
            // first we read null bits in the same value, if this is null, we
            // don't need to read value from page.
            // If this is not null, we read data from page in batch.
            // This would be bad in case that data is arranged one by one, which
            // will lead too many function calls to PageDecoder
            while (nrows_to_read > 0) {
                bool is_null = false;
                size_t this_run = _page.null_decoder.GetNextRun(&is_null, nrows_to_read);
                // we use num_rows only for CHECK
                size_t num_rows = this_run;
                if (!is_null) {
                    RETURN_IF_ERROR(_page.data_decoder->next_batch(&num_rows, dst));
                    DCHECK_EQ(this_run, num_rows);
                } else {
                    *has_null = true;
                }

                // set null bits
                dst->set_null_bits(this_run, is_null);

                nrows_to_read -= this_run;
                _page.offset_in_page += this_run;
                dst->advance(this_run);
                _current_ordinal += this_run;
            }
        } else {
            RETURN_IF_ERROR(_page.data_decoder->next_batch(&nrows_to_read, dst));
            DCHECK_EQ(nrows_to_read, nrows_in_page);

            if (dst->is_nullable()) {
                dst->set_null_bits(nrows_to_read, false);
            }

            _page.offset_in_page += nrows_to_read;
            dst->advance(nrows_to_read);
            _current_ordinal += nrows_to_read;
        }
        remaining -= nrows_in_page;
    }
    *n -= remaining;
    // TODO(hkp): for string type, the bytes_read should be passed to page decoder
    // bytes_read = data size + null bitmap size
    _opts.stats->bytes_read += *n * dst->type_info()->size() + BitmapSize(*n);
    return Status::OK();
}

Status FileColumnIterator::next_batch(size_t* n, vectorized::MutableColumnPtr& dst,
                                      bool* has_null) {
    size_t curr_size = dst->byte_size();
    size_t remaining = *n;
    *has_null = false;
    while (remaining > 0) {
        if (!_page.has_remaining()) {
            bool eos = false;
            RETURN_IF_ERROR(_load_next_page(&eos));
            if (eos) {
                break;
            }
        }

        // number of rows to be read from this page
        size_t nrows_in_page = std::min(remaining, _page.remaining());
        size_t nrows_to_read = nrows_in_page;
        if (_page.has_null) {
            while (nrows_to_read > 0) {
                bool is_null = false;
                size_t this_run = _page.null_decoder.GetNextRun(&is_null, nrows_to_read);
                // we use num_rows only for CHECK
                size_t num_rows = this_run;
                if (!is_null) {
                    RETURN_IF_ERROR(_page.data_decoder->next_batch(&num_rows, dst));
                    DCHECK_EQ(this_run, num_rows);
                } else {
                    *has_null = true;
                    auto* null_col =
                            vectorized::check_and_get_column<vectorized::ColumnNullable>(dst);
                    if (null_col != nullptr) {
                        const_cast<vectorized::ColumnNullable*>(null_col)->insert_null_elements(
                                this_run);
                    } else {
                        return Status::InternalError("unexpected column type in column reader");
                    }
                }

                nrows_to_read -= this_run;
                _page.offset_in_page += this_run;
                _current_ordinal += this_run;
            }
        } else {
            RETURN_IF_ERROR(_page.data_decoder->next_batch(&nrows_to_read, dst));
            DCHECK_EQ(nrows_to_read, nrows_in_page);

            _page.offset_in_page += nrows_to_read;
            _current_ordinal += nrows_to_read;
        }
        remaining -= nrows_in_page;
    }
    *n -= remaining;
    _opts.stats->bytes_read += (dst->byte_size() - curr_size) + BitmapSize(*n);
    return Status::OK();
}

Status FileColumnIterator::read_by_rowids(const rowid_t* rowids, const size_t count,
                                          vectorized::MutableColumnPtr& dst) {
    size_t remaining = count;
    size_t total_read_count = 0;
    size_t nrows_to_read = 0;
    while (remaining > 0) {
        RETURN_IF_ERROR(seek_to_ordinal(rowids[total_read_count]));

        // number of rows to be read from this page
        nrows_to_read = std::min(remaining, _page.remaining());

        if (_page.has_null) {
            size_t already_read = 0;
            while ((nrows_to_read - already_read) > 0) {
                bool is_null = false;
                size_t this_run = std::min(nrows_to_read - already_read, _page.remaining());
                if (UNLIKELY(this_run == 0)) {
                    break;
                }
                this_run = _page.null_decoder.GetNextRun(&is_null, this_run);
                size_t offset = total_read_count + already_read;
                size_t this_read_count = 0;
                rowid_t current_ordinal_in_page = _page.offset_in_page + _page.first_ordinal;
                for (size_t i = 0; i < this_run; ++i) {
                    if (rowids[offset + i] - current_ordinal_in_page >= this_run) {
                        break;
                    }
                    this_read_count++;
                }

                auto origin_index = _page.data_decoder->current_index();
                if (this_read_count > 0) {
                    if (is_null) {
                        auto* null_col =
                                vectorized::check_and_get_column<vectorized::ColumnNullable>(dst);
                        if (UNLIKELY(null_col == nullptr)) {
                            return Status::InternalError("unexpected column type in column reader");
                        }

                        const_cast<vectorized::ColumnNullable*>(null_col)->insert_null_elements(
                                this_read_count);
                    } else {
                        size_t read_count = this_read_count;

                        // ordinal in nullable columns' data buffer maybe be not continuously(the data doesn't contain null value),
                        // so we need use `page_start_off_in_decoder` to calculate the actual offset in `data_decoder`
                        size_t page_start_off_in_decoder =
                                _page.first_ordinal + _page.offset_in_page - origin_index;
                        RETURN_IF_ERROR(_page.data_decoder->read_by_rowids(
                                &rowids[offset], page_start_off_in_decoder, &read_count, dst));
                        DCHECK_EQ(read_count, this_read_count);
                    }
                }

                if (!is_null) {
                    _page.data_decoder->seek_to_position_in_page(origin_index + this_run);
                }

                already_read += this_read_count;
                _page.offset_in_page += this_run;
                DCHECK(_page.offset_in_page <= _page.num_rows);
            }

            nrows_to_read = already_read;
            total_read_count += nrows_to_read;
            remaining -= nrows_to_read;
        } else {
            RETURN_IF_ERROR(_page.data_decoder->read_by_rowids(
                    &rowids[total_read_count], _page.first_ordinal, &nrows_to_read, dst));
            total_read_count += nrows_to_read;
            remaining -= nrows_to_read;
        }
    }
    return Status::OK();
}

Status FileColumnIterator::_load_next_page(bool* eos) {
    _page_iter.next();
    if (!_page_iter.valid()) {
        *eos = true;
        return Status::OK();
    }

    RETURN_IF_ERROR(_read_data_page(_page_iter));
    _seek_to_pos_in_page(&_page, 0);
    *eos = false;
    return Status::OK();
}

Status FileColumnIterator::_read_data_page(const OrdinalPageIndexIterator& iter) {
    PageHandle handle;
    Slice page_body;
    PageFooterPB footer;
    _opts.type = DATA_PAGE;
    RETURN_IF_ERROR(_reader->read_page(_opts, iter.page(), &handle, &page_body, &footer,
                                       _compress_codec.get()));
    // parse data page
    RETURN_IF_ERROR(ParsedPage::create(std::move(handle), page_body, footer.data_page_footer(),
                                       _reader->encoding_info(), iter.page(), iter.page_index(),
                                       &_page));

    // dictionary page is read when the first data page that uses it is read,
    // this is to optimize the memory usage: when there is no query on one column, we could
    // release the memory of dictionary page.
    // note that concurrent iterators for the same column won't repeatedly read dictionary page
    // because of page cache.
    if (_reader->encoding_info()->encoding() == DICT_ENCODING) {
        auto dict_page_decoder = reinterpret_cast<BinaryDictPageDecoder*>(_page.data_decoder);
        if (dict_page_decoder->is_dict_encoding()) {
            if (_dict_decoder == nullptr) {
                // read dictionary page
                Slice dict_data;
                PageFooterPB dict_footer;
                _opts.type = INDEX_PAGE;
                RETURN_IF_ERROR(_reader->read_page(_opts, _reader->get_dict_page_pointer(),
                                                   &_dict_page_handle, &dict_data, &dict_footer,
                                                   _compress_codec.get()));
                // ignore dict_footer.dict_page_footer().encoding() due to only
                // PLAIN_ENCODING is supported for dict page right now
                _dict_decoder = std::make_unique<BinaryPlainPageDecoder<OLAP_FIELD_TYPE_VARCHAR>>(
                        dict_data);
                RETURN_IF_ERROR(_dict_decoder->init());

                auto* pd_decoder =
                        (BinaryPlainPageDecoder<OLAP_FIELD_TYPE_VARCHAR>*)_dict_decoder.get();
                _dict_word_info.reset(new StringRef[pd_decoder->_num_elems]);
                pd_decoder->get_dict_word_info(_dict_word_info.get());
            }

            dict_page_decoder->set_dict_decoder(_dict_decoder.get(), _dict_word_info.get());
        }
    }
    return Status::OK();
}

Status FileColumnIterator::get_row_ranges_by_zone_map(CondColumn* cond_column,
                                                      CondColumn* delete_condition,
                                                      RowRanges* row_ranges) {
    if (_reader->has_zone_map()) {
        RETURN_IF_ERROR(
                _reader->get_row_ranges_by_zone_map(cond_column, delete_condition, row_ranges));
    }
    return Status::OK();
}

Status FileColumnIterator::get_row_ranges_by_bloom_filter(CondColumn* cond_column,
                                                          RowRanges* row_ranges) {
    if (cond_column != nullptr && cond_column->can_do_bloom_filter() &&
        _reader->has_bloom_filter_index()) {
        RETURN_IF_ERROR(_reader->get_row_ranges_by_bloom_filter(cond_column, row_ranges));
    }
    return Status::OK();
}

Status DefaultValueColumnIterator::init(const ColumnIteratorOptions& opts) {
    _opts = opts;
    // be consistent with segment v1
    // if _has_default_value, we should create default column iterator for this column, and
    // "NULL" is a special default value which means the default value is null.
    if (_has_default_value) {
        if (_default_value == "NULL") {
            DCHECK(_is_nullable);
            _is_default_value_null = true;
        } else {
            _type_size = _type_info->size();
            _mem_value = reinterpret_cast<void*>(_pool->allocate(_type_size));
            Status s = Status::OK();
            if (_type_info->type() == OLAP_FIELD_TYPE_CHAR) {
                int32_t length = _schema_length;
                char* string_buffer = reinterpret_cast<char*>(_pool->allocate(length));
                memset(string_buffer, 0, length);
                memory_copy(string_buffer, _default_value.c_str(), _default_value.length());
                ((Slice*)_mem_value)->size = length;
                ((Slice*)_mem_value)->data = string_buffer;
            } else if (_type_info->type() == OLAP_FIELD_TYPE_VARCHAR ||
                       _type_info->type() == OLAP_FIELD_TYPE_HLL ||
                       _type_info->type() == OLAP_FIELD_TYPE_OBJECT ||
                       _type_info->type() == OLAP_FIELD_TYPE_STRING) {
                int32_t length = _default_value.length();
                char* string_buffer = reinterpret_cast<char*>(_pool->allocate(length));
                memory_copy(string_buffer, _default_value.c_str(), length);
                ((Slice*)_mem_value)->size = length;
                ((Slice*)_mem_value)->data = string_buffer;
            } else if (_type_info->type() == OLAP_FIELD_TYPE_ARRAY) {
                // TODO llj for Array default value
                return Status::NotSupported("Array default type is unsupported");
            } else {
                s = _type_info->from_string(_mem_value, _default_value);
            }
            if (!s.ok()) {
                return s;
            }
        }
    } else if (_is_nullable) {
        // if _has_default_value is false but _is_nullable is true, we should return null as default value.
        _is_default_value_null = true;
    } else {
        return Status::InternalError(
                "invalid default value column for no default value and not nullable");
    }
    return Status::OK();
}

Status DefaultValueColumnIterator::next_batch(size_t* n, ColumnBlockView* dst, bool* has_null) {
    if (dst->is_nullable()) {
        dst->set_null_bits(*n, _is_default_value_null);
    }

    if (_is_default_value_null) {
        *has_null = true;
        dst->advance(*n);
    } else {
        *has_null = false;
        for (int i = 0; i < *n; ++i) {
            memcpy(dst->data(), _mem_value, _type_size);
            dst->advance(1);
        }
    }
    return Status::OK();
}

void DefaultValueColumnIterator::insert_default_data(const TypeInfo* type_info, size_t type_size,
                                                     void* mem_value,
                                                     vectorized::MutableColumnPtr& dst, size_t n) {
    vectorized::Int128 int128;
    char* data_ptr = (char*)&int128;
    size_t data_len = sizeof(int128);

    switch (type_info->type()) {
    case OLAP_FIELD_TYPE_OBJECT:
    case OLAP_FIELD_TYPE_HLL: {
        dst->insert_many_defaults(n);
        break;
    }
    case OLAP_FIELD_TYPE_DATE: {
        assert(type_size == sizeof(FieldTypeTraits<OLAP_FIELD_TYPE_DATE>::CppType)); //uint24_t
        std::string str = FieldTypeTraits<OLAP_FIELD_TYPE_DATE>::to_string(mem_value);

        vectorized::VecDateTimeValue value;
        value.from_date_str(str.c_str(), str.length());
        value.cast_to_date();
        //TODO: here is int128 = int64, here rely on the logic of little endian
        int128 = binary_cast<vectorized::VecDateTimeValue, vectorized::Int64>(value);
        dst->insert_many_data(data_ptr, data_len, n);
        break;
    }
    case OLAP_FIELD_TYPE_DATETIME: {
        assert(type_size == sizeof(FieldTypeTraits<OLAP_FIELD_TYPE_DATETIME>::CppType)); //int64_t
        std::string str = FieldTypeTraits<OLAP_FIELD_TYPE_DATETIME>::to_string(mem_value);

        vectorized::VecDateTimeValue value;
        value.from_date_str(str.c_str(), str.length());
        value.to_datetime();

        int128 = binary_cast<vectorized::VecDateTimeValue, vectorized::Int64>(value);
        dst->insert_many_data(data_ptr, data_len, n);
        break;
    }
    case OLAP_FIELD_TYPE_DATEV2: {
        assert(type_size == sizeof(FieldTypeTraits<OLAP_FIELD_TYPE_DATEV2>::CppType)); //uint32_t

        int128 = *((FieldTypeTraits<OLAP_FIELD_TYPE_DATEV2>::CppType*)mem_value);
        dst->insert_many_data(data_ptr, data_len, n);
        break;
    }
    case OLAP_FIELD_TYPE_DECIMAL: {
        assert(type_size ==
               sizeof(FieldTypeTraits<OLAP_FIELD_TYPE_DECIMAL>::CppType)); //decimal12_t
        decimal12_t* d = (decimal12_t*)mem_value;
        int128 = DecimalV2Value(d->integer, d->fraction).value();
        dst->insert_many_data(data_ptr, data_len, n);
        break;
    }
    case OLAP_FIELD_TYPE_STRING:
    case OLAP_FIELD_TYPE_VARCHAR:
    case OLAP_FIELD_TYPE_CHAR: {
        data_ptr = ((Slice*)mem_value)->data;
        data_len = ((Slice*)mem_value)->size;
        dst->insert_many_data(data_ptr, data_len, n);
        break;
    }
    default: {
        data_ptr = (char*)mem_value;
        data_len = type_size;
        dst->insert_many_data(data_ptr, data_len, n);
    }
    }
}

Status DefaultValueColumnIterator::next_batch(size_t* n, vectorized::MutableColumnPtr& dst,
                                              bool* has_null) {
    *has_null = _is_default_value_null;
    _insert_many_default(dst, *n);
    return Status::OK();
}

Status DefaultValueColumnIterator::read_by_rowids(const rowid_t* rowids, const size_t count,
                                                  vectorized::MutableColumnPtr& dst) {
    _insert_many_default(dst, count);
    return Status::OK();
}

void DefaultValueColumnIterator::_insert_many_default(vectorized::MutableColumnPtr& dst, size_t n) {
    if (_is_default_value_null) {
        dst->insert_many_defaults(n);
    } else {
        insert_default_data(_type_info.get(), _type_size, _mem_value, dst, n);
    }
}

} // namespace segment_v2
} // namespace doris

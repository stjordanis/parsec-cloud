import pytest
import string
from hypothesis import given, strategies as st, note, assume

from parsec.core.fs.buffer_ordering import (
    merge_buffers,
    merge_buffers_with_limits,
    merge_buffers_with_limits_and_alignment,
    Buffer,
)


DATA_MAX_SIZE = 2 ** 8


class ColoredBuffer(Buffer):
    """
    We don't care about the buffer's data in those tests. However it makes
    debugging much easier if each buffer is made of a single caracter
    (mostly) per-buffer unique.
    """

    COLOR_CHOICES = string.ascii_lowercase + string.digits

    def __init__(self, start, end, data=None):
        if not data:
            color_key = (DATA_MAX_SIZE * start + end) % len(self.COLOR_CHOICES)
            color = self.COLOR_CHOICES[color_key]
            data = (color * (end - start)).encode()
        super().__init__(start, end, data)


buffer_size_strategy = st.integers(min_value=0, max_value=DATA_MAX_SIZE)
buffer_oversize_strategy = st.integers(min_value=0, max_value=int(DATA_MAX_SIZE * 1.2))
buffer_bounds_strategy = st.builds(sorted, st.tuples(buffer_size_strategy, buffer_size_strategy))
limits_strategy = st.builds(sorted, st.tuples(buffer_oversize_strategy, buffer_oversize_strategy))
buffer_strategy = st.builds(lambda x: ColoredBuffer(*x), buffer_bounds_strategy)


def _build_data_from_buffers(buffers, size=None):
    if size is None:
        size = max((b.end for b in buffers), default=0)
    data = bytearray(size)
    for b in buffers:
        data[b.start : b.end] = b.data
    return data


def _build_data_from_uncontiguous_space(ucs):
    data = bytearray(ucs.end)
    for cs in ucs.spaces:
        for buff in cs.buffers:
            data[buff.start : buff.end] = buff.buffer.data[
                buff.buffer_slice_start : buff.buffer_slice_end
            ]
    return data


def uncontigous_space_sanity_checks(ucs, expected_start, expected_end):
    previous_cs_end = -1
    for cs in ucs.spaces:
        assert cs.start >= expected_start
        assert cs.end <= expected_end

        assert cs.start >= previous_cs_end
        previous_cs_end = cs.end

        previous_bs_end = cs.start
        for bs in cs.buffers:
            assert bs.start == previous_bs_end
            previous_bs_end = bs.end

            assert bs.start >= bs.buffer.start
            assert bs.end <= bs.buffer.end


@given(buffers=st.lists(elements=buffer_strategy))
def test_merge_buffers(buffers):
    non_empty_buffers = [b for b in buffers if b.size]

    merged = merge_buffers(buffers)

    expected_start = min([b.start for b in non_empty_buffers], default=0)
    expected_end = max([b.end for b in non_empty_buffers], default=0)

    assert merged.start == expected_start
    assert merged.end == expected_end
    if merged.spaces:
        merged.spaces[0].start == merged.start
        merged.spaces[-1].end == merged.end
    uncontigous_space_sanity_checks(merged, expected_start, expected_end)

    expected = _build_data_from_buffers(non_empty_buffers)
    result = _build_data_from_uncontiguous_space(merged)

    assert result == expected


@given(buffers=st.lists(elements=buffer_strategy), limits=limits_strategy)
def test_merge_buffers_with_limits(buffers, limits):
    start, end = limits
    non_empty_buffers = [b for b in buffers if b.size]

    merged = merge_buffers_with_limits(buffers, start, end)

    expected_in_cs_min_start = min([b.start for b in non_empty_buffers], default=0)
    expected_in_cs_max_end = max([b.end for b in non_empty_buffers], default=0)

    assert merged.start == start
    assert merged.end == end
    if merged.spaces:
        merged.spaces[0].start == expected_in_cs_min_start
        merged.spaces[-1].end == expected_in_cs_max_end
    uncontigous_space_sanity_checks(merged, start, end)

    expected = _build_data_from_buffers(non_empty_buffers, end)
    result = _build_data_from_uncontiguous_space(merged)

    assert result[start:end] == expected[start:end]


def _contiguous_buffers_strategy_builder(buffers):
    def shift(offset, buffers):
        lower_buffer = min(buffers, key=lambda b: b.start)
        if lower_buffer.start > offset:
            shift_amount = lower_buffer.start - offset
            for b in buffers:
                b.start -= shift_amount
                b.end -= shift_amount
        return lower_buffer.end

    offset = 0
    need_shift = buffers
    while need_shift:
        offset = shift(offset, need_shift)
        need_shift = [b for b in need_shift if b.start > offset]
        if not need_shift:
            break
    return buffers


contiguous_buffers_strategy = st.builds(
    _contiguous_buffers_strategy_builder, st.lists(elements=buffer_strategy)
)


@given(
    buffers=contiguous_buffers_strategy,
    limits=limits_strategy,
    alignment=buffer_oversize_strategy.filter(lambda x: x != 0),
)
def test_merge_buffers_with_limits_and_alignment(buffers, limits, alignment):
    start, end = limits
    start = start - start % alignment
    non_empty_buffers = [b for b in buffers if b.size]

    merged = merge_buffers_with_limits_and_alignment(buffers, start, end, alignment)

    expected_in_cs_min_start = min([b.start for b in non_empty_buffers], default=0)
    expected_in_cs_max_end = max([b.end for b in non_empty_buffers], default=0)

    assert merged.start == start
    assert merged.end == end
    if merged.spaces:
        merged.spaces[0].start == expected_in_cs_min_start
        merged.spaces[-1].end == expected_in_cs_max_end
    uncontigous_space_sanity_checks(merged, start, end)

    expected = _build_data_from_buffers(non_empty_buffers, end)
    result = _build_data_from_uncontiguous_space(merged)

    assert result[start:end] == expected[start:end]
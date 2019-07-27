import datetime as dt
from enum import Enum
import itertools
import math
from types import SimpleNamespace
from typing import Tuple

month_abbr = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
combo_max = 2500


class SegmentType(Enum):
    FEATURE = 'feature'
    GEOHASH = 'geohash'
    YEAR = 'year'
    MONTH = 'month'
    DAY = 'day'
    HOURAM = 'hour'
    HOURPM = 'hour'
    RANGE = 'range'
    CALCULATION = 'calc'
    NONE = 'none'


class MalformedQueryError(Exception):
    """Exception raised for queries that cannot be parsed.

    Attributes:
        query -- input query that caused error
        message -- explanation of the error
    """
    def __init__(self, message, query=None):
        self.query = query
        self.message = message


class SummarizerError(Exception):
    """Exception raised when summarizer cannot return the proper value.

    Attributes:
        query -- input query that caused error
        message -- explanation of the error
    """

    def __init__(self, message, query=None):
        self.query = query
        self.message = message


def validate_builder_message(message: dict) -> Tuple[str, str]:
    try:
        query = message['query']
    except KeyError:
        raise MalformedQueryError('Missing query')

    try:
        stat = message['statistic']
    except KeyError:
        raise MalformedQueryError('Missing statistic')

    # TODO: Remove hard coded statistics
    if stat not in {'mean', 'max', 'min', 'stddev', 'variance', 'size', 'base'}:  # skewness, kurtosis?
        raise MalformedQueryError('Invalid statistic')

    return query, stat


def _duplicate_error_msg(first, second, segtype='') -> str:
    return f'Multiple {segtype} inputs: {first} and {second}. Please only specify one of each type of input. Use ' \
        f'[start:end] for ranges.'


def _check_duplicate(parts: SimpleNamespace, seg_type: SegmentType, segment: str,  query: str = None) -> None:
    if hasattr(parts, seg_type.value):
        raise MalformedQueryError(_duplicate_error_msg(getattr(parts, seg_type.value), segment, seg_type.value), query)


def segment_type(segment: str) -> SegmentType:
    segment = segment.strip()
    if len(segment) <= 0:
        return SegmentType.NONE

    if segment[0] == '[' and segment[-1] == ']':
        return SegmentType.RANGE
    elif segment[0] == '(' and segment[-1] == ')':
        return SegmentType.CALCULATION
    elif len(segment) >= 2 and segment[:-2].isdigit():
        if segment.isdigit():
            return SegmentType.YEAR
        else:
            if segment[-2:] == 'AM':
                return SegmentType.HOURAM
            elif segment[-2:] == 'PM':
                return SegmentType.HOURPM
            else:
                return SegmentType.DAY
    else:
        if segment[0] == '@':
            return SegmentType.GEOHASH
        elif segment in month_abbr:
            return SegmentType.MONTH
        else:
            return SegmentType.FEATURE


# Converting int to ordinal from https://stackoverflow.com/a/20007730
def ordinal(n: int) -> str:
    return "%d%s" % (n, "tsnrhtdd"[(math.floor(n / 10) % 10 != 1) * (n % 10 < 4) * n % 10::4])


def ampmfrom24(x: int) -> str:
    return str(x-12)+'PM' if x > 12 else str(x)+'AM'


def _range_to_list(rangestr: str, parts: SimpleNamespace) -> list:
    if ':' not in rangestr:
        raise MalformedQueryError(f'{rangestr} is not a range')
    rangeseg = rangestr.split(':')
    if len(rangeseg) != 2:
        raise MalformedQueryError(f'Too many values in {rangestr}. Ranges can only have 2 arguments: [start:end].')
    rangetype = segment_type(rangeseg[0])

    now = dt.datetime.now()
    if rangetype == SegmentType.CALCULATION:
        rangeseg[0] = rangeseg[0][1:-1]
        sub = rangeseg[0].split('-')
        if len(sub) == 1 and len(sub[0]) == 1 and sub[0][0] == '$':
            segtype = segment_type(rangeseg[1])
            if segtype == SegmentType.YEAR:
                rangeseg[0] = str(now.year)
            elif segtype == SegmentType.MONTH:
                rangeseg[0] = month_abbr[now.month - 1]
            elif segtype == SegmentType.DAY:
                rangeseg[0] = ordinal(now.day)
            elif segtype.value == SegmentType.HOURAM.value:
                rangeseg[0] = ampmfrom24(now.hour)
            else:
                raise MalformedQueryError(f'Unable to parse {rangeseg[0]}. Does not match type of {rangeseg[1]}.')
        elif len(sub) != 2:
            raise MalformedQueryError(f'Unable to calculate {rangeseg[0]}.')
        else:
            try:
                # TODO: check that the val being removed is less than the current time to prevent negative numbers
                #       and index out of range
                val = int(sub[1][:-1])
                if sub[1][-1] == 'y':
                    rangeseg[0] = str(now.year - val)
                elif sub[1][-1] == 'm':
                    rangeseg[0] = month_abbr[now.month - val - 1]
                elif sub[1][-1] == 'd':
                    rangeseg[0] = ordinal(now.day - val)
                elif sub[1][-1] == 'h':
                    rangeseg[0] = ampmfrom24(now.hour - val)
                else:
                    raise MalformedQueryError(f'Unable to parse value {sub[1][-1]} in {rangeseg[0]}.')
            except ValueError:
                raise MalformedQueryError(f'Unable to parse equation {rangeseg[0]}.')
        rangetype = segment_type(rangeseg[0])
    if segment_type(rangeseg[1]) == SegmentType.CALCULATION:
        rangeseg[1] = rangeseg[1][1:-1]
        sub = rangeseg[1].split('-')
        if len(sub) == 1 and len(sub[0]) == 1 and sub[0][0] == '$':
            segtype = segment_type(rangeseg[0])
            if segtype == SegmentType.YEAR:
                rangeseg[1] = str(now.year)
            elif segtype == SegmentType.MONTH:
                rangeseg[1] = month_abbr[now.month - 1]
            elif segtype == SegmentType.DAY:
                rangeseg[1] = ordinal(now.day)
            elif segtype.value == SegmentType.HOURAM.value:
                rangeseg[1] = ampmfrom24(now.hour)
            else:
                raise MalformedQueryError(f'Unable to parse {rangeseg[1]}. Does not match type of {rangeseg[0]}.')
        elif len(sub) != 2:
            raise MalformedQueryError(f'Unable to calculate {rangeseg[1]}.')
        else:
            try:
                val = int(sub[1][:-1])
                # TODO: check that the val being removed is less than the current time to prevent negative numbers
                #       and index out of range
                if sub[1][-1] == 'y':
                    rangeseg[1] = str(now.year - val)
                elif sub[1][-1] == 'm':
                    rangeseg[1] = month_abbr[now.month - val - 1]
                elif sub[1][-1] == 'd':
                    rangeseg[1] = ordinal(now.day - val)
                elif sub[1][-1] == 'h':
                    rangeseg[1] = ampmfrom24(now.hour - val)
                else:
                    raise MalformedQueryError(f'Unable to parse value {sub[1][-1]} in {rangeseg[1]}.')
            except ValueError:
                raise MalformedQueryError(f'Unable to parse equation {rangeseg[1]}.')

    if rangetype.value != segment_type(rangeseg[1]).value:
        raise MalformedQueryError(f'{rangeseg[0]} and {rangeseg[1]} are different types. '
                                  f'{segment_type(rangeseg[0]).value} and {segment_type(rangeseg[1]).value} '
                                  f'respectively')

    if rangetype == SegmentType.FEATURE:
        raise MalformedQueryError(f'Features are not supported with ranges. Feature: {rangeseg[0]}.')
    elif rangetype == SegmentType.YEAR:
        if int(rangeseg[0]) > int(rangeseg[1]):
            raise MalformedQueryError(f'Invalid range: {rangestr}. Did you mean: [{rangeseg[1]}:{rangeseg[0]}]?')
        return list(str(x) for x in range(int(rangeseg[0]), int(rangeseg[1])+1))
    elif rangetype == SegmentType.MONTH:
        month1 = month_abbr.index(rangeseg[0])
        month2 = month_abbr.index(rangeseg[1])
        if month1 > month2:
            raise MalformedQueryError(f'Invalid range: {rangestr}. Did you mean: [{rangeseg[1]}:{rangeseg[0]}]?')
        return month_abbr[month1: month2+1]
    elif rangetype == SegmentType.DAY:
        day1 = int(rangeseg[0][:-2])
        day2 = int(rangeseg[1][:-2])
        if day1 > 31 or day1 < 1 or day2 > 31 or day2 < 1:
            raise MalformedQueryError(f'Date is out of range: {rangestr}.')
        if day1 > day2:
            raise MalformedQueryError(f'Invalid range: {rangestr}. Did you mean: [{rangeseg[1]}:{rangeseg[0]}]?')
        return list(ordinal(d) for d in range(day1, day2+1))
    elif rangetype == SegmentType.HOURAM or rangetype == SegmentType.HOURPM:
        # TODO: Make sure the time is in range
        if int(rangeseg[0][:-2]) > 12 or int(rangeseg[0][:-2]) < 1 or int(rangeseg[1][:-2]) > 12 \
                or int(rangeseg[1][:-2]) < 1:
            raise MalformedQueryError(f'Time is out of range: {rangestr}.')
        ampmto24 = lambda x: 12+int(x[:-2]) if x[-2:] == 'PM' else int(x[:-2])
        hour1 = ampmto24(rangeseg[0])
        hour2 = ampmto24(rangeseg[1])
        if hour1 > hour2:
            raise MalformedQueryError(f'Invalid range: {rangestr}. Did you mean: [{rangeseg[1]}:{rangeseg[0]}]?')
        return list(ampmfrom24(x) for x in range(hour1, hour2+1))


def parse_range(parts: SimpleNamespace, segment: str) -> None:
    assert segment[0] == '[' and segment[-1] == ']'
    segment = segment[1:-1]
    if len(segment) <= 0:
        # Only [] which is not useful... So do nothing.
        return
    multi = segment.split(',')
    if ':' not in multi[0]:
        segtype = segment_type(multi[0])
    else:
        segtype = segment_type(_range_to_list(multi[0], parts)[0])
    temp = list()
    for s in multi:
        if ':' not in s:
            if segtype.value != segment_type(s).value:
                raise MalformedQueryError(f'Invalid selection in {segment}. {multi[0]} is {segtype.value} but {s} is '
                                          f'{segment_type(s)}')
            temp.append(s)
        else:
            r = _range_to_list(s, parts)
            if segment_type(r[0]).value != segtype.value:
                raise MalformedQueryError(f'Invalid range in {segment}. {multi[0]} is {segtype.value} but {s} is '
                                          f'{segment_type(r[0])}')
            for x in r:
                temp.append(x)
    setattr(parts, segtype.value, list(dict.fromkeys(temp).keys()))


def _parse_calc(parts: SimpleNamespace, segment: str) -> None:
    assert (segment[0] == '(' and segment[-1] == ')')
    segment = segment[1:-1]
    now = dt.datetime.now()
    if segment[0] != '$':
        raise MalformedQueryError(f'Unable to calculate {segment}.')
    if len(segment) == 1:  # Only $
        if not hasattr(parts, SegmentType.YEAR.value):
            parts.year = str(now.year)
        if not hasattr(parts, SegmentType.MONTH.value):
            parts.month = month_abbr[now.month - 1]
        if not hasattr(parts, SegmentType.DAY.value):
            parts.day = ordinal(now.day)
        if not hasattr(parts, SegmentType.HOURPM.value):
            parts.hour = ampmfrom24(now.hour)
    else:
        sub = segment.split('-')
        if len(sub) != 2:
            raise MalformedQueryError(f'Unable to find equation {segment}.')
        try:
            # TODO: check that the val being removed is less than the current time to prevent negative numbers
            #       and index out of range
            val = int(sub[1][:-1])
            if sub[1][-1] == 'y':
                _check_duplicate(parts, SegmentType.YEAR, segment)
                parts.year = str(now.year - val)
            elif sub[1][-1] == 'm':
                _check_duplicate(parts, SegmentType.MONTH, segment)
                parts.month = month_abbr[now.month - val - 1]
            elif sub[1][-1] == 'd':
                _check_duplicate(parts, SegmentType.DAY, segment)
                parts.day = ordinal(now.day - val)
            elif sub[1][-1] == 'h':
                _check_duplicate(parts, SegmentType.HOURAM, segment)
                parts.hour = ampmfrom24(now.hour - val)
            else:
                raise MalformedQueryError(f'Unable to parse value {sub[1][-1]} in {segment}.')
        except ValueError:
            raise MalformedQueryError(f'Unable to parse equation {segment}.')


def _is_hour12(hour: int) -> bool:
    return 0 <= hour <= 12


def parse_segment(parts: SimpleNamespace, segment: str, query: str = None) -> None:
    if len(segment) <= 0:
        # If segment is blank, do nothing
        return

    seg_type = segment_type(segment)
    if seg_type == SegmentType.RANGE:
        parse_range(parts, segment)
    elif seg_type == SegmentType.CALCULATION:
        _parse_calc(parts, segment)
    elif seg_type == SegmentType.YEAR or seg_type == SegmentType.DAY or seg_type == SegmentType.MONTH or \
            seg_type == SegmentType.FEATURE:
        # TODO: Validate that the date is properly written
        _check_duplicate(parts, seg_type, segment, query)
        setattr(parts, seg_type.value, segment)
    elif seg_type == SegmentType.HOURAM or seg_type == SegmentType.HOURPM:
        _check_duplicate(parts, seg_type, segment, query)
        if not _is_hour12(int(segment[:-2])):
            raise MalformedQueryError(f'Hour ({segment}) is out of range.', query)
        parts.hour = segment
    elif seg_type == SegmentType.GEOHASH:
        # Multiple different geohashes are currently disallowed
        if hasattr(parts, seg_type.value):
            parts.geohash += segment[1:]
        else:
            parts.geohash = segment
    # elif seg_type == SegmentType.NONE: do nothing


def preprocess_query(query: str) -> Tuple[SimpleNamespace, list]:
    # TODO: Optimize such that it does not need to parse the entire query
    # TODO: Consider returning a dict instead and using the date as the key
    # queries = dict()

    parts = SimpleNamespace()

    for segment in query.split('.'):
        parse_segment(parts, segment, query)
    # OLD version that just returns list of strings
    # return parts, ['.'.join(s) for s in itertools.product(
    #     *list(map(lambda x: [x] if not isinstance(x, list) else x, list(filter(None, vars(parts).values())))))]
    d = dict(vars(parts).items())
    return parts, list(dict(zip(d, x)) for x in list(itertools.islice(
        itertools.product(*map(lambda x: x if isinstance(x, list) else [x], list(filter(None, d.values())))),
        combo_max)))

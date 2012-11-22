import logging
from base import BabeBase, StreamMeta, StreamHeader, StreamFooter


def join(stream, join_stream, key, join_key, add_fields=None, on_error=BabeBase.ON_ERROR_WARN):
    d = {}
    join_header = None
    normalized_join_key = StreamHeader.keynormalize(join_key)
    normalized_key = StreamHeader.keynormalize(key)
    for row in join_stream:
        if isinstance(row, StreamHeader):
            join_header = row
            logging.debug(join_header)
        elif isinstance(row, StreamFooter):
            break
        else:
            k = getattr(row, normalized_join_key)
            if not k in d:
                d[k] = row

    for row in stream:
        if isinstance(row, StreamHeader):
            if add_fields:
                fields = add_fields
            else:
                fields = [field for field in join_header.normalized_fields
                        if field != normalized_join_key]
            header = row.insert(typename=None, fields=fields)
            yield header
        elif isinstance(row, StreamMeta):
            yield row
        else:
            k = getattr(row, normalized_key)
            if k in d:
                dd = row._asdict()
                jrow = d[k]
                for field in header.normalized_fields:
                    dd[field] = getattr(jrow, field, dd.get(field))
                yield header.t(**dd)
            else:
                if on_error == BabeBase.ON_ERROR_WARN:
                    BabeBase.log_warn("join", row, "Not matching value for key")
                elif on_error == BabeBase.ON_ERROR_FAIL:
                    raise Exception("No matching value for key %s" % k)
                elif on_error == BabeBase.ON_ERROR_NONE:
                    dd = row._asdict()
                    for f in header.normalized_fields:
                        dd[f] = dd.get(f)
                    yield header.t(**dd)
                elif on_error == BabeBase.ON_ERROR_SKIP:
                    pass


BabeBase.register("join", join)

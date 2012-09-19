
import json
import codecs
from base import StreamHeader, BabeBase, StreamFooter


def pull(format, stream, kwargs):
    stream = codecs.getreader(kwargs.get('encoding', 'utf8'))(stream)

    previous_fields = None
    for line in stream:
        data = json.loads(line)
        fields = data.keys()
        if previous_fields != fields:
            metainfo = StreamHeader(**dict(kwargs, fields=fields))
            previous_fields = fields
            yield metainfo
        yield metainfo.t._make(data.values())
    yield StreamFooter()


def push(format, metainfo, instream, outfile, encoding, **kwargs):
    outstream = codecs.getwriter(kwargs.get('encoding', 'utf8'))(outfile)
    for row in instream:
        if isinstance(row, StreamFooter):
            break
        elif isinstance(row, StreamHeader):
            metainfo = row
        else:
            outstream.write(json.dumps(dict(zip(metainfo.fields, row))))
            outstream.write("\n")
    outstream.flush()

BabeBase.addPullPlugin('json', ['json'], pull)
BabeBase.addPushPlugin('json', ['json'], push)

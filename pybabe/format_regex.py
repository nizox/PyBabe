import re
import codecs
import logging

from base import StreamHeader, BabeBase, StreamFooter


def pull(format, stream, kwargs):
    stream = codecs.getreader(kwargs.get('encoding', 'utf8'))(stream)

    regex = kwargs.get('regex')
    if isinstance(regex, basestring):
        regex = re.compile(regex)

    logging.debug(regex.pattern)

    if len(regex.groupindex) == 0:
        fields = ["%i" % i for i in range(regex.groups)]
    else:
        fields = [n for n, i in sorted(regex.groupindex.items(),
            key=lambda x: x[1])]
    metainfo = StreamHeader(fields=kwargs.get('fields', fields))

    logging.debug(metainfo.fields)

    yield metainfo

    for line in stream:
        matches = regex.match(line)
        if matches is not None:
            yield metainfo.t._make(matches.groups())
        else:
            logging.debug("line does not match %s", line)
    yield StreamFooter()

BabeBase.addPullPlugin('regex', ['txt'], pull)

import time

import feedparser


def encode_feedparser_dict(d):
    """helper function to strip feedparser objects using a deep copy"""
    if isinstance(d, feedparser.FeedParserDict) or isinstance(d, dict):
        return {
            k: encode_feedparser_dict(d[k])
            for k in d.keys()
            if not isinstance(d[k], time.struct_time)
        }
    elif isinstance(d, list):
        return [encode_feedparser_dict(k) for k in d]
    else:
        return d


def atom_parse(response):
    parsed = feedparser.parse(response)
    return encode_feedparser_dict(parsed)

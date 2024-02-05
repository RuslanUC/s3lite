from xml.etree.ElementTree import Element


class CaseInsensitiveDict(dict):
    def __init__(self, d: dict = None, **data):
        super().__init__()
        for k, v in data.items():
            self[k] = v
        if isinstance(d, dict):  # pragma: no cover
            for k, v in d.items():
                self[k] = v

    def __setitem__(self, key: str, value):
        super(CaseInsensitiveDict, self).__setitem__(key.lower(), value)

    def __getitem__(self, key: str):  # pragma: no cover
        return super(CaseInsensitiveDict, self).__getitem__(key.lower())

    def __contains__(self, item: str):
        return super().__contains__(item.lower())


NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"
NS_URL = NS[1:-1]


def get_xml_attr(element: Element, name: str, get_all: bool = False, ns: str = NS) -> Element:
    path = f".//{ns}{name}"
    return element.findall(path) if get_all else element.find(path)

import json
from helper_function.hf_string import to_json_str


class JsonObj:
    def __init__(self, *args, **kwargs):
        pass

    def to_json_obj_raw(
            self,
            include_attrs=(),
            exclude_attrs=()
    ):

        res = dict()
        if len(include_attrs) == 0 or include_attrs == 'all':
            include_attrs = list(
                self.__dir__()[:list(self.__dir__()).index('__module__')]
            )

        include_attrs = sorted(
            list(set(include_attrs) - set(exclude_attrs)),
            key=lambda x: include_attrs.index(x)
        )

        for attr in include_attrs:
            value = eval('self.%s' % attr)
            if isinstance(value, (list, tuple, set)):
                try:
                    res[attr] = []
                except AttributeError:
                    continue

                for v in value:
                    if v:
                        try:
                            res[attr].append(
                                v.to_json_obj_raw(include_attrs=include_attrs)
                            )
                        except AttributeError:
                            res[attr].append(v)

            elif isinstance(value, dict):
                try:
                    res[attr] = dict()
                except AttributeError:
                    continue
                for k, v in value.items():
                    try:
                        res[attr][k] = v.to_json_obj_raw(include_attrs=include_attrs)
                    except AttributeError:
                        res[attr][k] = v

            else:
                try:
                    res[attr] = value.to_json_obj_raw(include_attrs=include_attrs)
                except AttributeError:
                    res[attr] = value

        return res

    def to_json(self, include_attrs=(), exclude_attrs=()):
        jo = self.to_json_obj_raw(
            include_attrs=include_attrs,
            exclude_attrs=exclude_attrs
        )
        return to_json_str(jo)

    def to_json_obj(self, include_attrs=(), exclude_attrs=()):
        return json.loads(
            self.to_json(include_attrs=include_attrs, exclude_attrs=exclude_attrs)
        )

import json
from hf_string import to_json_str
from typing import Set, List


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


def get_nodes(relation_info):
    res = list(set(sum(relation_info, start=[])))
    res = sorted([item for item in res if item])
    return res


def get_graph(relation_info):
    graph = dict()
    for node, parent in relation_info:
        if parent is not None:
            try:
                graph[node].add(parent)
            except KeyError:
                graph[node] = {parent}
        else:
            graph[node] = set()
    for k in graph.keys():
        graph[k] = sorted(list(graph[k]))
    return graph


def get_related_nodes(graph, node, visited=None):
    if visited is None:
        visited = set()

    parents = set(graph[node])
    children = set()
    for node_in_graph, ps in graph.items():
        if node in ps and node_in_graph not in visited:
            children.add(node_in_graph)
            visited.add(node_in_graph)
            children |= set(get_related_nodes(graph, node_in_graph, visited))

    return sorted(list({node} | parents | children))


def depth_first_search(
        node: str,
        graph: dict,
        visited: Set[str],
        stack: List[str]
):
    visited.add(node)
    for parent in graph[node]:
        if parent not in visited:
            depth_first_search(parent, graph, visited, stack)
    stack.append(node)


def topological_sort(relation_info: list, reverse=False) -> List[str]:
    nodes = get_nodes(relation_info=relation_info)
    graph = get_graph(relation_info=relation_info)
    visited = set()
    stack = []
    for node in nodes:
        if node not in visited:
            depth_first_search(node, graph, visited, stack)

    if reverse:
        return [node for node in reversed(stack)]
    else:
        return [node for node in stack]

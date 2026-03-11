from typing import Dict, List, Set
from collections import defaultdict

from src.utils.logger import get_logger

logger = get_logger("lineage_builder")


class LineageBuilder:
    def __init__(self):
        self.nodes: Dict[str, Dict] = {}
        self.edges: List[Dict] = []
        self._forward: Dict[str, Set[str]] = defaultdict(set)   # source -> targets
        self._backward: Dict[str, Set[str]] = defaultdict(set)   # target -> sources

    def build_from_parsed_programs(self, parsed_programs: List[Dict]) -> Dict:
        self.nodes = {}
        self.edges = []
        self._forward = defaultdict(set)
        self._backward = defaultdict(set)

        for prog in parsed_programs:
            prog_id = f"program:{prog['filename']}"
            self._add_node(prog_id, "program", prog["filename"])

            # Datasets read
            for ds in prog.get("datasets_read", []):
                ds_id = f"dataset:{ds.lower()}"
                self._add_node(ds_id, "dataset", ds)
                self._add_edge(ds_id, prog_id, "reads")

            # Datasets written
            for ds in prog.get("datasets_written", []):
                ds_id = f"dataset:{ds.lower()}"
                self._add_node(ds_id, "dataset", ds)
                self._add_edge(prog_id, ds_id, "writes")

            # Macro definitions
            for macro in prog.get("macro_definitions", []):
                macro_id = f"macro:{macro.lower()}"
                self._add_node(macro_id, "macro", macro)
                self._add_edge(prog_id, macro_id, "defines")

            # Macro calls
            for macro in prog.get("macro_calls", []):
                macro_id = f"macro:{macro.lower()}"
                self._add_node(macro_id, "macro", macro)
                self._add_edge(macro_id, prog_id, "calls")

            # Includes
            for inc in prog.get("includes", []):
                inc_id = f"include:{inc}"
                self._add_node(inc_id, "include", inc)
                self._add_edge(inc_id, prog_id, "includes")

        logger.info("Lineage built: %d nodes, %d edges", len(self.nodes), len(self.edges))
        return {
            "nodes": list(self.nodes.values()),
            "edges": self.edges,
        }

    def _add_node(self, node_id: str, node_type: str, label: str):
        if node_id not in self.nodes:
            self.nodes[node_id] = {"id": node_id, "type": node_type, "label": label}

    def _add_edge(self, source: str, target: str, relationship: str):
        edge = {"source": source, "target": target, "relationship": relationship}
        if edge not in self.edges:
            self.edges.append(edge)
            self._forward[source].add(target)
            self._backward[target].add(source)

    def get_upstream(self, node_id: str) -> List[str]:
        visited = set()
        self._traverse_backward(node_id, visited)
        visited.discard(node_id)
        return list(visited)

    def get_downstream(self, node_id: str) -> List[str]:
        visited = set()
        self._traverse_forward(node_id, visited)
        visited.discard(node_id)
        return list(visited)

    def _traverse_backward(self, node_id: str, visited: Set[str]):
        if node_id in visited:
            return
        visited.add(node_id)
        for src in self._backward.get(node_id, set()):
            self._traverse_backward(src, visited)

    def _traverse_forward(self, node_id: str, visited: Set[str]):
        if node_id in visited:
            return
        visited.add(node_id)
        for tgt in self._forward.get(node_id, set()):
            self._traverse_forward(tgt, visited)

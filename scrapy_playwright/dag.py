"""Lightweight request dependency DAG backed by an adjacency list.

The ``AdjacencyList`` class is a generic directed graph that can be reused
outside of the DAG scheduler (e.g. for duplicate-link tracking).
``RequestDAG`` layers DAG semantics on top: cycle detection, timeout
tracking, and a ``DependencyResult`` payload that flows from resolved
dependencies back to their dependents.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Generic, List, Optional, Set, TypeVar

from scrapy.http import Request, Response

logger = logging.getLogger("scrapy-playwright")

# ---------------------------------------------------------------------------
# Generic adjacency list
# ---------------------------------------------------------------------------

TVertex = TypeVar("TVertex")
TEdge = TypeVar("TEdge")


class AdjacencyList(Generic[TVertex, TEdge]):
    """Directed graph stored as an indexed list of nodes."""

    class Node(Generic[TVertex, TEdge]):
        vertex: TVertex
        connections: dict[int, TEdge]

        def __init__(self, vertex: TVertex) -> None:
            super().__init__()
            self.vertex = vertex
            self.connections = dict()

    nodes: list["AdjacencyList.Node[TVertex, TEdge]"]

    def __init__(self) -> None:
        self.nodes = list()

    def add_vertex(self, vertex: TVertex) -> int:
        index = len(self.nodes)
        self.nodes.append(self.Node(vertex))
        return index

    def add_edge(self, origin: int, destination: int, edge: TEdge) -> None:
        self.nodes[origin].connections[destination] = edge

    def has_path(self, start: int, end: int) -> bool:
        """Return True if *end* is reachable from *start* via DFS."""
        visited: Set[int] = set()
        stack = [start]
        while stack:
            node = stack.pop()
            if node == end:
                return True
            if node in visited:
                continue
            visited.add(node)
            stack.extend(self.nodes[node].connections.keys())
        return False

    def __len__(self) -> int:
        return len(self.nodes)


# ---------------------------------------------------------------------------
# DAG structures
# ---------------------------------------------------------------------------

@dataclass
class DependencyResult:
    """Outcome of a resolved dependency request."""
    request: Request
    response: Optional[Response] = None
    error: Optional[Exception] = None

    @property
    def ok(self) -> bool:
        return self.response is not None and self.error is None


@dataclass
class _PendingRequest:
    """Internal record for a request waiting on dependencies."""
    request: Request
    future: asyncio.Future
    dependencies: List[str]  # fingerprints of dependency requests
    results: Dict[str, DependencyResult] = field(default_factory=dict)
    created_at: float = field(default_factory=time.monotonic)


class CycleError(Exception):
    """Adding this dependency would create a cycle."""


class RequestDAG:
    """Manages dependency edges between Scrapy ``Request`` objects.

    Each request is identified by its fingerprint (``request.url`` by
    default — callers may supply a custom keying function).

    *   ``add_dependency(dependent, dependency)`` registers an edge.
        Raises ``CycleError`` if the edge would create a cycle.
    *   ``park(request)`` returns an ``asyncio.Future`` that resolves
        once all dependencies of *request* have been satisfied.
    *   ``resolve(request, response)`` marks a dependency as complete
        and unblocks any dependents.
    *   ``fail(request, error)`` marks a dependency as failed.
    """

    def __init__(self, timeout: float = 90.0) -> None:
        self._graph: AdjacencyList[str, None] = AdjacencyList()
        self._index: Dict[str, int] = {}  # fingerprint → vertex index
        self._pending: Dict[str, _PendingRequest] = {}  # fingerprint → pending
        self._timeout = timeout

    # -- helpers ------------------------------------------------------------

    @staticmethod
    def _fingerprint(request: Request) -> str:
        return request.url

    def _ensure_vertex(self, fp: str) -> int:
        if fp not in self._index:
            self._index[fp] = self._graph.add_vertex(fp)
        return self._index[fp]

    # -- public API ---------------------------------------------------------

    def add_dependency(self, dependent: Request, dependency: Request) -> None:
        dep_fp = self._fingerprint(dependent)
        req_fp = self._fingerprint(dependency)
        dep_idx = self._ensure_vertex(dep_fp)
        req_idx = self._ensure_vertex(req_fp)

        # Cycle check: if dependency can already reach dependent, adding
        # dependent → dependency would close a cycle.
        if self._graph.has_path(req_idx, dep_idx):
            raise CycleError(
                f"Adding {dep_fp!r} → {req_fp!r} would create a cycle"
            )
        self._graph.add_edge(dep_idx, req_idx, None)

    def park(self, request: Request) -> asyncio.Future:
        """Park *request* until all its dependencies resolve."""
        fp = self._fingerprint(request)
        if fp in self._pending:
            return self._pending[fp].future

        idx = self._index.get(fp)
        dep_fps: List[str] = []
        if idx is not None:
            for dest_idx in self._graph.nodes[idx].connections:
                dep_fps.append(self._graph.nodes[dest_idx].vertex)

        loop = asyncio.get_event_loop()
        future: asyncio.Future = loop.create_future()
        if not dep_fps:
            future.set_result({})
            return future

        self._pending[fp] = _PendingRequest(
            request=request,
            future=future,
            dependencies=dep_fps,
        )
        return future

    def resolve(self, request: Request, response: Response) -> None:
        """Mark *request* as successfully completed; unblock dependents."""
        fp = self._fingerprint(request)
        result = DependencyResult(request=request, response=response)
        self._notify(fp, result)

    def fail(self, request: Request, error: Exception) -> None:
        """Mark *request* as failed; unblock dependents with error."""
        fp = self._fingerprint(request)
        result = DependencyResult(request=request, error=error)
        self._notify(fp, result)

    def _notify(self, fp: str, result: DependencyResult) -> None:
        for pending_fp, pending in list(self._pending.items()):
            if fp in pending.dependencies:
                pending.results[fp] = result
                if len(pending.results) == len(pending.dependencies):
                    if not pending.future.done():
                        pending.future.set_result(pending.results)
                    del self._pending[pending_fp]

    def check_timeouts(self) -> List[Request]:
        """Fail any parked requests whose timeout has elapsed. Returns the
        list of timed-out requests."""
        now = time.monotonic()
        timed_out: List[Request] = []
        for fp, pending in list(self._pending.items()):
            if now - pending.created_at > self._timeout:
                if not pending.future.done():
                    pending.future.set_exception(
                        TimeoutError(f"Dependency timeout for {fp}")
                    )
                timed_out.append(pending.request)
                del self._pending[fp]
        return timed_out

    @property
    def pending_count(self) -> int:
        return len(self._pending)

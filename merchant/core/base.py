from __future__ import annotations


class Identifiable:
    def __hash__(self) -> int:
        return hash(self)

    def __eq__(self, __o: object) -> bool:
        return self is __o

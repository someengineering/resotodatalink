from dataclasses import dataclass


@dataclass
class EngineConfig:
    connection_string: str
    batch_size: int = 5000

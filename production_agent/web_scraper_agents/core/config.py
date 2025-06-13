from dataclasses import dataclass, asdict
from enum import Enum
import logging
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta

# correlation_id could be job_id or a combination of any type of identityfier
# configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)as - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s'
)

class LoggerAdapter(logging.LoggerAdapter):
    def process (self, msg, kwargs):
        return f"[{self.extra["correlation_id"]}] {msg}", kwargs
    

# data models
    
class AgentStatus(Enum):
    IDLE="idle"
    RUNNING="running"
    ERROR="error"
    STOPPED="stopped"


# use dataclass to auto generate __init__, __repr__, __eq__ built in methods, cleaner, maintainable
@dataclass
class ScrapingTask:
    url:str
    site_name:str
    task_id:str
    max_retires: int=3
    timeout:int=30

@dataclass
class ScrapingResult:
    task_id:str
    site_name: str
    url: str
    data:Optional[Dict[str, Any]]
    error: Optional[str]
    timestamp: datetime
    duration: float

@dataclass
class AgentMetrics:
    agent_id:str
    status: AgentStatus
    tasks_completed:int
    tasks_failed:int
    avg_response_time:float
    last_activity:datetime


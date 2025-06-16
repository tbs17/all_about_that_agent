from dataclasses import dataclass, asdict
from enum import Enum
import logging
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta
import sys
# correlation_id could be job_id or a combination of any type of identityfier
# configure logging

handler=logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)

# Creat a formatter
formatter=logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s')
handler.setFormatter(formatter)

# Get the root logger and set the level
root_logger=logging.getLogger()
root_logger.setLevel(logging.INFO)

# ADD THE HANDLER TO THE ROOT LOGGER

root_logger.addHandler(handler)

class LoggerAdapter(logging.LoggerAdapter):
# loggerAdapter could inject extra data into log records before they are processed by handlers
    # you can format any additional records into basicConfig,but if it's not default in there, you need to adapt/inject
    def process (self, msg, kwargs):
        correlation_id=self.extra.get('correlation_id','n/a')
    
        kwargs['extra']=kwargs.get('extra',{})
        kwargs['extra']['correlation_id']=correlation_id
        # return f'[{self.extra["correlation_id"]}] {msg}', kwargs
        return msg, kwargs


# data models
    
class AgentStatus(Enum):
    IDLE="idle"
    RUNNING="running"
    ERROR="error"
    STOPPED="stopped"

@dataclass
class AgentMetrics:
    agent_id:str
    status: AgentStatus
    tasks_completed:int
    tasks_failed:int
    avg_response_time:float
    last_activity:datetime



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


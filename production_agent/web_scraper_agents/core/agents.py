from abc import ABC, abstractclassmethod
import asyncio
from contextlib import asynccontextmanager
import logging
from config import LoggerAdapter, AgentMetrics,AgentStatus
from typing import Dict, List, Any, Optional, Callable
import weakref
import time
# Base Agent class

class BaseAgent(ABC):
    def __init__(self, agent_id:str, correlation_id:str):
        self.agent_id=agent_id
        self.correlation_id=correlation_id
        self.logger=LoggerAdapter(logging.getLogger(self.__class__.__name__),
                                  {'correlation_id':correlation_id})
        self.status=AgentStatus.IDLE
        self._shutdown_event=asyncio.Event()
        self._tasks:weakref.WeakSet=weakref.WeakSet() #the regular set() is  a strong reference and even when task is finished, it stay in the tasks set forever
        # WeakSet can automatically free up the memory when task is completed

    @abstractclassmethod #forces the subclass must implement these methods
    async def start (self):
        pass

    @abstractclassmethod
    async def stop (self):
        pass

# shutdown doesn't need to be modified in the subclass because its beahvior works for 90%+ of the agent
    async def shutdown(self):

        """graceful shutdown"""
        self.logger.info(f"shutting down agent {self.agent_id}")
        self._shutdown_event.set()
        await self.stop()


        # cancel all running taks
        for task in list(self._tasks):
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass



# Rate Limiter Agent

class RateLimiterAgent(BaseAgent):
    """limit on global rate limit and site limits"""
    def __init__(self, agent_id: str, correlation_id: str, global_rate_limit:int=10,
                 site_limits:Dict[str, int]=None):
        super().__init__(agent_id, correlation_id)
        self.global_semaphore=asyncio.Semaphore(global_rate_limit)
        self.site_semaphore={}
        self.site_limits=site_limits or {}
        self._request_times={}

    
    async def start(self):
        self.status=AgentStatus.RUNNING
        self.logger.info("Rate Limiter agent started")

    async def stop(self):
        self.status=AgentStatus.STOPPED
        self.logger.info("Rate Limiter agent stopped")
    
    @asynccontextmanager
    async def acquire_permit(self,site_name:str):
        """acquire rate limiting permit for a site"""
        # get or create site-specific semaphore

        if site_name not in self.site_semaphore: #different sites have different rate limits: amazon: max 2 requests/sec while ebay:3requests/sec and small sites: 5 reqests/site
            limit=self.site_limits.get(site_name,5) #look up the limit for the site, default to 5 if not specified
            self.site_semaphore[site_name]=asyncio.Semaphore(limit)
        site_semaphore=self.site_semaphores[site_name]

        async with self.global_semaphore:
            async with site_semaphore:
                # implement rate limiting with time-based delays
                now=time.time()
                last_request=self._request_times.get(site_name,0)
                min_interval=1.0 # minimum 1 second between requests
                if now-last_request<min_interval:
                    delay=min_interval-(now-last_request)
                    await asyncio.sleep(delay) #apply a small delay between requests but doesn't block other tasks executing

                self._request_times[site_name]=time.time()
                yield # yield here is giving permission to the following request, remember this class is to acquire permission
import config
from config import LoggerAdapter, AgentMetrics,AgentStatus, ScrapingTask,ScrapingResult
from abc import ABC, abstractclassmethod
import asyncio
import aiohttp
from dataclasses import asdict
from contextlib import asynccontextmanager
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Callable
import weakref
import time
from datetime import datetime
import sys
import json,os
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
#============= Base Agent class===================



class BaseAgent(ABC):
    def __init__(self, agent_id:str, correlation_id:str):
        self.agent_id=agent_id
        self.correlation_id=correlation_id
        # you will access the root logger 
        logger=logging.getLogger(self.__class__.__name__) #name the logger to be different class
        logger.setLevel(logging.INFO) #set the logger level
        self.logger=LoggerAdapter(logger,
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



#================== Rate Limiter Agent===================

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
        site_semaphore=self.site_semaphore[site_name]

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

# =================the main scraper agent=======================   
class ScraperAgent(BaseAgent):
    def __init__(self, agent_id:str, correlation_id:str,
                    rate_limiter:RateLimiterAgent, result_queue:asyncio.Queue):
        super().__init__(agent_id, correlation_id)
        self.rate_limiter=rate_limiter
        self.result_queue=result_queue
        self.task_queue=asyncio.Queue()
        self.session: Optional[aiohttp.ClientSession]=None
        self.metrics=AgentMetrics(
            agent_id=agent_id,
            status=AgentStatus.IDLE,
            tasks_completed=0,
            tasks_failed=0,
            avg_response_time=0.0,
            last_activity=datetime.now()
        )
        self._response_times=[]
        self.output_dir=Path("outputs")
        self.output_dir.mkdir(exist_ok=True)
        # circuit breaker state
        # the purpose of using circuit breaker is to prevent repeatedly retrying on a problematic sites due to downtime or mailfunction of their server
        # to avoid resource wasting
        self.failure_count=0
        self.failure_threshold=5
        self.recovery_timeout=60 #after 60 seconds, the scraper will try again
        self.circuit_open=False #initial state is circuit closed, so scrapper can continuously scrape

    async def start(self):
        self.status=AgentStatus.RUNNING
        self.session=aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            connector=aiohttp.TCPConnector(limit=10,limit_per_host=5) #TCPConnector is used to reuse existing connections and reduce latency

        )
            
        # start worker task
        worker_task=asyncio.create_task(self._worker())
        self._tasks.add(worker_task)

        self.logger.info(f"Scraper agent {self.agent_id} started")

    async def stop(self):
        self.status=AgentStatus.STOPPED
        if self.session:
            await self.session.close()
        self.logger.info(f"Scraper agent {self.agent_id} stopped")
    
    async def add_task(self, task: ScrapingTask):
        """add a scraping task to the queue"""
        await self.task_queue.put(task)
    
    def _is_circuit_open(self)-> bool:
        """check if the circuit breaker is open"""
        if not self.circuit_open:
            return False
        
        if self.last_failure_time and time.time()-self.last_failure_time>self.recovery_timeout: 
            # whenever the elapsed time bigger than the recovery_timeout, then circuit needs to try again to scrape
            self.circuit_open=False
            self.failure_count=0
            self.logger.info("Circuit breaker reset")
            return False
        return True
    
    def _record_success(self, response_time:float):
        """record successful request"""
        self.failure_count=0
        self.circuit_open=False
        self.metrics.tasks_completed+=1
        self._response_times.append(response_time)


        # keep only last 100 response times for average calculation
        if len(self._response_times)>100:
            self._response_times.pop(0)
        
        self.metrics.avg_response_time=sum(self._response_times)/len(self._response_times)
        self.metrics.last_activity=datetime.now()
    
    def _record_failure(self):
        """record failed request"""
        self.failure_count+=1
        self.metrics.tasks_failed+=1
        self.last_failure_time=time.time()
        if self.failure_count>=self.failure_threshold:
            self.circuit_open=True
            self.logger.warning(f'Circuit breaker opened for agent {self.agent_id}')
    
    async def _save_json(self, result: ScrapingResult):
        """Save single result as JSON"""
        filename = f"{result.task_id}_{result.site_name}.json"
        
        filepath = self.output_dir / filename
        
        result_dict = {
            "task_id": result.task_id,
            "site_name": result.site_name,
            "url": result.url,
            "data": result.data,
            "error": result.error,
            "timestamp": result.timestamp.isoformat(),
            "duration": result.duration
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(result_dict, f, indent=2, ensure_ascii=False)

    async def _scrape_with_retry(self,task:ScrapingTask)->ScrapingResult:
        """scrape with exponential backoff retry"""
        start_time=time.time()

        for attempt in range (task.max_retires+1):
            if self._shutdown_event.is_set():
                raise asyncio.CancelledError
            if self._is_circuit_open():
                return ScrapingResult(
                    task_id=task.task_id,
                    site_name=task.site_name,
                    url=task.url,
                    data=None,
                    error="Circuit breaker open",
                    timestamp=datetime.now(),
                    duration=time.time()-start_time

                )

            try:
                async with self.rate_limiter.acquire_permit(task.site_name):
                    async with self.session.get(task.url) as response:
                        if response.status==200:
                            # simulate data extraction
                            data={
                                "title":f"Product from {task.site_name}",
                                "price": "$99.99",
                                "status_code":response.status,
                                "scraped_at":datetime.now().isoformat()

                            }
                            duration=time.time()-start_time
                            self._record_success(duration)

                            result= ScrapingResult(
                                task_id=task.task_id,
                                site_name=task.site_name,
                                url=task.url,
                                data=data,
                                error=None,
                                timestamp=datetime.now(),
                                duration=duration
                            )
                            
                            self.logger.info("Start saving scraped results")
                            await self._save_json(result)
                            self.logger.info("Saved scraped results")
                        else:
                            raise aiohttp.ClientConnectionError(
                                request_info=response.request_info,
                                history=response.history,
                                status=response.status
                            )

            except asyncio.CancelledError:
                raise 
            except Exception as e:
                self.logger.warning(f"Attempt {attempt+1} failed for {task.url}:{str(e)}")
                if attempt<task.max_retires:
                    # exponential backoff with jitter
                    delay=(2**attempt)+(time.time()%1)
                    await asyncio.sleep(delay)
                else:
                    self._record_failure()
                    return ScrapingResult(
                        task_id=task.task_id,
                        site_name=task.site_name,
                        url=task.url,
                        data=None,
                        error=str(e),
                        timestamp=datetime.now(),
                        duration=time.time()-start_time
                    )
    async def _worker(self):
        """main worker loop"""
        while not self._shutdown_event.is_set():
            try:
                # wait for task with timeout to allow shutdown
                task=await asyncio.wait_for(self.task_queue.get(),
                                            timeout=1.0)
                self.logger.info(f"Processing task {task.task_id} for {task.site_name}")
                result=await self._scrape_with_retry(task)
                await self.result_queue.put(result)
            
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Worker error: {str(e)}")
        

# ===================Progress Reporter Agent===============

class ProgressReporterAgent(BaseAgent):
    """report fail and completed tasks"""
    def __init__(self, agent_id: str, correlation_id: str, result_queue:asyncio.Queue, agents:List[ScraperAgent]):
        super().__init__(agent_id, correlation_id)
        self.result_queue=result_queue
        self.agents=agents
        self.total_tasks=0
        self.completed_tasks=0
        self.failed_tasks=0
        self.results=[]

    async def start(self):
        self.status=AgentStatus.RUNNING

        # start progress reporter
        processor_task=asyncio.create_task(self._process_results())
        self._tasks.add(processor_task)

        self.logger.info("Progress reporter started")

    async def stop(self):
        self.status=AgentStatus.STOPPED
        self.logger.info("Progress reporter stopped")

    def set_total_tasks(self,total:int):
        self.total_tasks=total
    
    async def _process_results(self):
        """process scraping results"""

        while not self._shutdown_event.is_set():
            try:
                # wait for the results get collected for a second
                result=await asyncio.wait_for(
                    self.result_queue.get(),
                    timeout=1.0
                )
                if result.error:
                    self.failed_tasks+=1
                    self.logger.error(f"Task {result.task_id} failed: {result.error}")
                else:
                    self.completed_tasks+=1
                    self.logger.info(f"Task {result.task_id} completed successfully")
            except asyncio.TimeoutError:
                continue
            
            except asyncio.CancelledError:
                break
    
    async def _report_progress(self):
        """report progress periodically"""

        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(5)
                progress={
                    "total_tasks":self.total_tasks,
                    "completed_tasks":self.completed_tasks,
                    "failed_tasks": self.failed_tasks,
                    "progress_percentage": (self.completed_tasks+self.failed_tasks)/max(self.total_tasks,1)*100,
                    "agent_metrics": [asdict(agent.metrics) for agent in self.agents],
                    "timestamp":datetime.now().isoformat
                }

                self.logger.info(f"Progress:{progress['progress_percentage']:.1f}%"
                                 f"({self.completed_tasks}/{self.total_tasks} completed, "
                                 f"{self.failed_tasks} failed)"
                                 )
            except asyncio.CancelledError:
                break


# ========Agent Manager: Main coordinator=========

class AgentManager:
    def __init__(self, correlation_id:str, num_scrapers:int=3):
        self.correlation_id=correlation_id
        self.logger=LoggerAdapter(
            logging.getLogger(self.__class__.__name__),
            {'correlation_id':correlation_id}
        )
        
        # initialize agents
        self.result_queue=asyncio.Queue()
        self.rate_limiter=RateLimiterAgent(
            "rate_limiter",
            correlation_id,
            global_rate_limit=10,
            site_limits={"amazon.com":2,
                         "ebay.com":3,
                         "etsy.com":5}
             
        )
        self.scrapers=[
            ScraperAgent(
                f"scraper_{i}",correlation_id,
                self.rate_limiter,
                self.result_queue
            )
            for i in range(num_scrapers)
        ]
        self.progress_reporter=ProgressReporterAgent(
            "progress_reporter",
            correlation_id,
            self.result_queue,
            self.scrapers
        )

        self.all_agents=[self.rate_limiter]+self.scrapers+[self.progress_reporter]

    async def start_all_agents(self):
        self.logger.info("Starting all agents...")
        start_tasks=[agent.start() for agent in self.all_agents]
        await asyncio.gather(*start_tasks)
        self.logger.info("All agents started successfully")

    
    async def stop_all_agents(self):
        """stop all agents gracefully"""
        self.logger.info("Stopping all agents...")

        # signal shutdown to all agents
        shutdown_tasks=[agent.shutdown() for agent in self.all_agents]
        await asyncio.gather(*shutdown_tasks, return_exceptions=True)

        self.logger.info("All agents stopped")

    async def execute_scraping_job(self, tasks: List[ScrapingTask])->List[ScrapingResult]:
        """execte a batcg if scraping tasks"""

        try:
            await self.start_all_agents()
            self.progress_reporter.set_total_tasks(len(tasks))
            self.logger.info(f"Starting scraping job with {len(tasks)} tasks")

            # distribute tasks among scrapers
            for i, task in enumerate(tasks):
                scraper=self.scrapers[i % len(self.scrapers)]
                await scraper.add_task(task)
            # wait for all tasks to complete
            while (self.progress_reporter.completed_tasks + self.progress_reporter.failed_tasks)<len(tasks):
                await asyncio.sleep(1)
            self.logger.info("All tasks completed")
            return self.progress_reporter.results
        
        except Exception as e:
            self.logger.error(f"Job execution failed:{str(e)}")
            raise
        finally:
            await self.stop_all_agents()


def create_etsy_tasks():
    base_url = "https://www.etsy.com/r/themes/1314285282186"
    
    # Different query parameter combinations
    tasks = []
    
    # Original URL
    original_params = {
        'anchor_listings': '1683424418',
        'ref': 'hp_themes_module-2'
    }
    
    # Create variations (if needed)
    param_variations = [
        original_params,
        {'anchor_listings': '1683424418'},  # Without ref
        {'ref': 'hp_themes_module-2'},      # Without anchor_listings
        {}  # No parameters
    ]
    
    for i, params in enumerate(param_variations):
        if params:
            query_string = urlencode(params)
            full_url = f"{base_url}?{query_string}"
        else:
            full_url = base_url
            
        tasks.append(ScrapingTask(full_url, "etsy.com", f"task_etsy_{i}"))
    
    return tasks


async def main():
    correlation_id=f"job_{int(time.time())}"

    # create sample tasks
    # the below url won't work as they are not legit, but replace with the valid url,it will work
    # Use in your main function
    etsy_tasks = create_etsy_tasks()    
    tasks=etsy_tasks
    # tasks=[*[
    #     ScrapingTask(f'https://amazon.com/product/{i}',"amazon.com",f"task_{i}") for i in range(5)
    # ],*[
    #     ScrapingTask(f"https://ebay.com/item/{i}","ebay.com",f"task_{i+5}") for i in range(3)
    # ],*[
    #     ScrapingTask(f"https://etsy.com/listing/1777495153/{i}","etsy.com",f"task_{i+8}") for i in range(2)
    # ]]

    # executing scraping job
    manager=AgentManager(correlation_id,num_scrapers=3)

    try:
        results=await manager.execute_scraping_job(tasks)

        # print summary
        successful=[r for r in results if r.error is None]
        failed=[r for r in results if r.error is not None]

        print(f"\n ===scraping job summary===")
        print(f'Total tasks: {len(tasks)}')
        print(f'Successful: {len(successful)}')
        print(f'Failed: {len(failed)}')
        print(f"Success rate: {len(successful)/len(tasks)*100:.1f}%")

        if successful:
            avg_duration=sum(r.duration for r in successful) /len(successful)
            print(f'Average response time: {avg_duration:.2f}s')
        

    except KeyboardInterrupt:
        print("\nReceived interrupt signal, shuttign down ...")
        await manager.stop_all_agents()


if __name__=="__main__":
    asyncio.run(main())
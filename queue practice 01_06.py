#queue practice 01
#status: improve 01 (status: success)
#try 06
 
import asyncio
from random import uniform,randint
from typing import Tuple,List

async def producer(ID:int):
    try:
        #to simulate data get time
        await asyncio.sleep(uniform(0.1,1.5))
        job=randint(1,9)
        return job
    except asyncio.CancelledError:
        #to find out what ID was not produced now
        print(f"producer cancelled on ID: {ID}\n(job with (ID: {ID}) was not produced)")
        raise

async def producer_with_retry(q:asyncio.Queue,IDs_list:List[int],*,wait_time:float=1,try_count:int=3):
    #to continue the program based on lengh IDs_list
    count=len(IDs_list)
    while count>0:
        count-=1
        #for gettig next ID
        ID=IDs_list.pop()
        #for refresh tc by try_count
        tc=try_count
        while tc>0:
            tc-=1
            try:
                #for discuss in except asyncio.CancelledError
                job=False
                #obtaining job
                job=await asyncio.wait_for(producer(ID),timeout=wait_time)
                #putting job
                await q.put((ID,job))
                break
            except asyncio.TimeoutError:
                print(f"retry produce of job (ID: {ID})")
                continue
            except asyncio.CancelledError:
                print("producer cancelled")
                #for cancelling on putting job
                if job:
                    #for don't lossing getted job
                    await q.put((ID,job))
                #for cancelling on obtaining job
                else:
                    #for don't lossing ID , after the program continued after it was canceled
                    IDs_list.append(ID)
                    #for correct count , after the program continued after it was canceled
                    count+=1
                raise
        else:
            print(f"failed to produce job with (ID: {ID})")



async def consumer(job,processing_time:Tuple[float]):
    l=processing_time[0]
    m=processing_time[1]
    #to Simulate processing time
    await asyncio.sleep(uniform(l,m))
    return job*2

async def consumer_with_retry(q:asyncio.Queue,fast_q:asyncio.Queue,*,wait_time:float=3,try_count:int=5):
    try:
        #for use again of try_count
        tc=try_count
        while True:
            while tc>0:
                try:
                    #for discuss in finally
                    answer=False
                    job_with_ID=False
                    f=False
                    #getting job
                    if not fast_q.empty():
                        job_with_ID=await fast_q.get()
                        f=True
                    else:
                        job_with_ID=await  q.get()
                    job=job_with_ID[1]
                    ID=job_with_ID[0]
                    #obtaining answer
                    answer=await asyncio.wait_for(consumer(job,(1,3.5)),timeout=wait_time)
                    print(f"process of job with (ID: {ID}) finished\n(ID: {ID} | answer: {answer})")
                    break
                except asyncio.TimeoutError:
                    print(f"retry consumer of job with (ID: {ID})")
                finally:
                    if job_with_ID:
                        #for Error on getting job
                        if f:
                            fast_q.task_done()
                        else:
                            q.task_done()
                        #for Error on obtaining answer
                        if not answer:
                            #first thing to get
                            await fast_q.put(job_with_ID)
                            tc-=1
            #for finish of tc for obtaining answer
            else:
                print(f"failed to consumer job with (ID: {ID})")
            #for refresh tc by try_count
            tc=try_count
    except asyncio.CancelledError:
        print("consumer cancelled")
        raise


async def shutdown_manager(count:int=15):
    IDs_list=[i for i in range(count,0,-1)]
    fast_q=asyncio.Queue(maxsize=3)
    q=asyncio.Queue(maxsize=5)
    producer_task=asyncio.create_task(producer_with_retry(q,IDs_list=IDs_list))
    consumer_task=asyncio.create_task(consumer_with_retry(q,fast_q))
    await producer_task
    await q.join()
    await fast_q.join()
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        print("closing consumer")
    
asyncio.run(shutdown_manager())

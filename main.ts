import { Job, Queue, Worker } from 'bullmq';
import { formatDistanceToNowStrict } from 'date-fns';
import IORedis from 'ioredis';
import _ from 'lodash';
import * as asciichart from 'asciichart'
const connection = new IORedis({
  maxRetriesPerRequest: null
});

const mainQueue = new Queue('main', {
  connection,
  defaultJobOptions: {
    attempts: 3, 
    backoff: {
      type: 'fixed',
      delay: 50
    },
    removeOnComplete: true,
    removeOnFail: false
  }
  
})

mainQueue.on('error', (error) => {
  console.error(error)
})
function registerPriorityInterval({ priority, intervalInMS }: { intervalInMS: number; priority: number }) {
  console.log(`Registering interval with priority ${priority} and interval ${intervalInMS}`);

  const intervalId = setInterval(() => {
    const jobName = `Job Priority ${priority}`;
    mainQueue.add(jobName, { priority }, { priority }).catch(error => console.error(error))
  }, intervalInMS)
  return intervalId
}


const priorityMap = new Map([
  [1, 240],
  [2, 120],
  [3, 100]
])
async function main() {
  await mainQueue.drain()
  const startedAt = new Date()
  const interval_ids = Array.from(priorityMap.entries()).map(([priority, intervalInMS]) => {
    return registerPriorityInterval({ priority, intervalInMS })
  })
  let prioritizedJobsMap = new Map<number, number[]>()
  let initialProcessedCount = 0
  let previousPendingAmount = 0
  const printerIntevalId = setInterval(async () => {
    const jobs = await mainQueue.getPrioritized()
    const pendingJobsCount = jobs.length
    const groupedJobs = _.groupBy(jobs, (job) => job.opts.priority ?? job.data.priority ?? 0)
    for (const key in groupedJobs) {
      prioritizedJobsMap.set(+key, [...(prioritizedJobsMap.get(+key) ?? []), groupedJobs[key].length])
    }
    console.clear()
    console.log(`Pending jobs : ${pendingJobsCount}, at ${pendingJobsCount - previousPendingAmount} jobs per second`)
    previousPendingAmount = pendingJobsCount
    console.log(priorityMapCount)
    const totalProcessedCount = Array.from(priorityMapCount.values()).reduce((acc, count) => acc + count, 0);
    console.log(`Total Processed jobs: ${totalProcessedCount}`)
    console.log(`Processed jobs last second: ${totalProcessedCount - initialProcessedCount}`)
    const oldestJob = _.minBy(jobs, (job) => job.timestamp)
    if (oldestJob?.timestamp) {
      console.log(`Max job age: ${formatDistanceToNowStrict(new Date(oldestJob.timestamp))}, priority: ${oldestJob.opts.priority}, id: ${oldestJob.id}, created at: ${new Date(oldestJob.timestamp).toISOString()}`)
    }
    console.log(`Execution duration: ${formatDistanceToNowStrict(startedAt)}`)
    const config = {
      height: 10,
      colors: [
        asciichart.blue,
        asciichart.green,
        asciichart.red,
      ],
      offset: 2
      
    }
    const jobSeries = Array.from(prioritizedJobsMap.values());
    console.log(asciichart.plot(jobSeries, config))
    console.log(await mainQueue.getMetrics('failed'))
    initialProcessedCount = totalProcessedCount
  }, 1000)

  process.on('SIGTERM', () => {
    interval_ids.forEach(interval_id => clearInterval(interval_id))
    clearInterval(printerIntevalId)
  })
}

main()

const priorityMapCount = new Map([
  [1, 0],
  [2, 0],
  [3, 0]
])


const worker = new Worker('main', async job => {
  // Will print { foo: 'bar'} for the first job
  // and { qux: 'baz' } for the second.
  await new Promise((resolve, reject) => {
    priorityMapCount.set(job.data.priority, (priorityMapCount.get(job.data.priority) ?? 0) + 1)
    setTimeout(async () => {
      const seed = Math.random()
      if (job.attemptsMade > 0) {
          // console.log(`Retrying job with attempts made ${job.attemptsMade} and priority ${job.opts.priority}`)
      }
      if (seed >= 0.575) {
        if (job.attemptsMade > 0) {
          // console.log(`Failing job on ${job.attemptsMade + 1} attempt`)
        }
        reject(new Error('Failed Jobs'))
      }
      resolve(null)
    }, 30)
  })
}, {
  connection, concurrency: 30, limiter: {
    duration: 1000,
    max: 30
  },
  useWorkerThreads: true,
  metrics: {
    maxDataPoints: Number.MAX_SAFE_INTEGER
  }
});
